"""DAG factory for gold layer production (silver PG → gold PG).

Generates one ``{dataset}_to_gold`` DAG per gold dataset in the data catalog.
Each DAG is triggered when all upstream ``to_silver_pg`` DAGs have produced new data,
and runs ``dbt run`` then ``dbt test`` inside the Airflow container.
"""

import subprocess
from datetime import timedelta

import orjson
from airflow.sdk import DAG, Asset, AssetAll, dag, task

from data_eng_etl_electricity_meteo.airflow.assets import get_gold_pg_asset, get_silver_pg_asset
from data_eng_etl_electricity_meteo.airflow.defaults import DEFAULT_ARGS, START_DATE
from data_eng_etl_electricity_meteo.core.data_catalog import DataCatalog, GoldDatasetConfig
from data_eng_etl_electricity_meteo.core.exceptions import InvalidCatalogError
from data_eng_etl_electricity_meteo.core.logger import get_logger
from data_eng_etl_electricity_meteo.core.settings import settings

logger = get_logger("dag.to_gold")


# --------------------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------------------


_DBT_COMMON_ARGS: list[str] = [
    "--project-dir",
    str(settings.dbt_project_dir),
    "--profiles-dir",
    str(settings.dbt_project_dir),
    "--target",
    settings.dbt_target,
    "--log-path",
    str(settings.dbt_log_path),
    "--target-path",
    str(settings.dbt_target_path),
    "--log-format",
    "json",
]

TASK_DBT_RUN = "dbt_run"
TASK_DBT_TEST = "dbt_test"

TASK_DBT_RUN_TIMEOUT = timedelta(minutes=15)
TASK_DBT_TEST_TIMEOUT = timedelta(minutes=10)

_DBT_LOG_LEVELS = {
    "debug": logger.debug,
    "info": logger.info,
    "warn": logger.warning,
    "warning": logger.warning,
    "error": logger.error,
}


# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------


def _run_dbt(subcommand: str, extra_args: list[str] | None = None) -> None:
    """Run a dbt subcommand, streaming JSON log lines through structlog."""
    cmd = ["dbt", subcommand, *_DBT_COMMON_ARGS, *(extra_args or [])]
    logger.info("Starting dbt", dbt_cmd=subcommand)
    logger.debug("dbt full command", command=cmd)

    with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True) as proc:
        assert proc.stdout is not None  # guaranteed by PIPE
        for raw_line in proc.stdout:
            line = raw_line.rstrip()
            if not line:
                continue
            try:
                event = orjson.loads(line)
                meta = event.get("info", {})
                msg = meta.get("msg", line)
                level = meta.get("level", "info").lower()
                log_fn = _DBT_LOG_LEVELS.get(level, logger.info)
                log_fn(msg, dbt_cmd=subcommand, dbt_code=meta.get("code"))
            except orjson.JSONDecodeError:
                logger.debug(line, dbt_cmd=subcommand)

    if proc.returncode != 0:
        logger.error("dbt failed", dbt_cmd=subcommand, returncode=proc.returncode)
        raise subprocess.CalledProcessError(proc.returncode, cmd)

    logger.info("dbt finished successfully", dbt_cmd=subcommand)


# --------------------------------------------------------------------------------------
# DAG factory
# --------------------------------------------------------------------------------------


def _create_dag(
    dataset: GoldDatasetConfig,
    gold_asset: Asset,
    schedule: Asset | AssetAll,
) -> DAG:
    """Create a dbt DAG for a single gold dataset.

    Parameters
    ----------
    dataset
        Gold dataset configuration from the catalog.
    gold_asset
        Outlet: the gold Postgres Asset emitted after successful dbt test.
    schedule
        Inlet: ``AssetAll`` of upstream silver PG Assets (or single Asset).

    Returns
    -------
    DAG
        Instantiated DAG object.
    """

    @dag(
        dag_id=f"{dataset.name}_to_gold",
        schedule=schedule,
        start_date=START_DATE,
        catchup=False,  # TODO[prod]: set to True
        default_args=DEFAULT_ARGS,
        tags=["dbt", "gold", "postgres"],
        description=dataset.description[:200] if dataset.description else None,
        doc_md=__doc__,
    )
    def _dag() -> None:
        @task(
            task_id=TASK_DBT_RUN,
            execution_timeout=TASK_DBT_RUN_TIMEOUT,
        )
        def dbt_run() -> None:
            """Execute ``dbt run --select +{model}`` (model + upstream)."""
            # '+' includes upstream silver staging views
            _run_dbt(subcommand="run", extra_args=["--select", f"+{dataset.name}"])

        @task(
            task_id=TASK_DBT_TEST,
            execution_timeout=TASK_DBT_TEST_TIMEOUT,
            outlets=[gold_asset],
        )
        def dbt_test() -> None:
            """Execute ``dbt test --select {model}``."""
            _run_dbt(subcommand="test", extra_args=["--select", dataset.name])

        dbt_run() >> dbt_test()  # for IDE

    return _dag()


def _generate_all_dags() -> dict[str, DAG]:
    """Generate dbt DAGs for all gold datasets in the catalog.

    Returns
    -------
    dict[str, DAG]
        Mapping of dataset names to their dbt DAG objects.
    """
    try:
        catalog = DataCatalog.load(path=settings.data_catalog_file_path)
    except InvalidCatalogError as e:
        e.log(logger.critical)
        return {}

    dags: dict[str, DAG] = {}

    for dataset in catalog.get_gold_datasets():
        try:
            gold_asset = get_gold_pg_asset(dataset)
            upstream_assets = [
                get_silver_pg_asset(catalog.get_remote_dataset(dep))
                for dep in dataset.source.depends_on
            ]
            schedule: Asset | AssetAll = (
                upstream_assets[0] if len(upstream_assets) == 1 else AssetAll(*upstream_assets)
            )
        except ValueError:
            logger.exception("Invalid dataset configuration", dataset=dataset.name)
            continue  # move to next dataset

        dags[dataset.name] = _create_dag(dataset, gold_asset, schedule)
        logger.info("to_gold DAG created", dataset=dataset.name)

    total = len(catalog.get_gold_datasets())
    logger.info("to_gold factory complete", created_count=len(dags), total_count=total)

    return dags


# Airflow discovers DAGs via @dag decorator; return value is intentionally unused.
_ = _generate_all_dags()
