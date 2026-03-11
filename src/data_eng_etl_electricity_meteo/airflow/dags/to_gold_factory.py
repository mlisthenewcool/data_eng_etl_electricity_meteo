"""DAG factory for gold layer production (silver PG → gold PG).

Generates one ``{dataset}_to_gold`` DAG per gold dataset in the data catalog.
Each DAG is triggered when all upstream ``to_silver_pg`` DAGs have produced new data,
and runs ``dbt run`` then ``dbt test`` inside the Airflow container.
"""

import subprocess
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Literal

import orjson
from airflow.sdk import DAG, Asset, AssetAll, chain, dag, task

from data_eng_etl_electricity_meteo.airflow.assets import get_gold_pg_asset, get_silver_pg_asset
from data_eng_etl_electricity_meteo.airflow.defaults import DEFAULT_ARGS, START_DATE
from data_eng_etl_electricity_meteo.core.data_catalog import DataCatalog, GoldDatasetConfig
from data_eng_etl_electricity_meteo.core.exceptions import (
    DataCatalogError,
    GoldStageError,
    InvalidCatalogError,
)
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
    "--log-path",
    str(settings.dbt_log_path),
    "--target-path",
    str(settings.dbt_target_path),
    "--log-format",
    "json",
    "--no-use-colors",
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

# dbt event codes with rich data worth extracting.
# Q007: test result, Q012: model result, Q034: skip on error.
_DBT_RESULT_CODES = {"Q007", "Q012", "Q034"}


# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _DbtResult:
    """Parsed dbt model/test result event (codes Q007/Q012/Q034)."""

    node_name: str
    status: str
    progress: str | None
    duration_s: float | None
    rows_count: int | None
    adapter_msg: str | None

    @staticmethod
    def from_raw(data: dict[str, Any]) -> "_DbtResult | None":
        """Parse a dbt ``data`` dict into a typed result.

        Returns ``None`` when mandatory fields (``node_name``, ``status``) are missing
        or have unexpected types.
        """
        node_info = data.get("node_info")
        if not isinstance(node_info, dict):
            return None
        node_name = node_info.get("node_name")
        status = data.get("status")
        if not isinstance(node_name, str) or not isinstance(status, str):
            return None

        index = data.get("index")
        total = data.get("total")
        progress = f"{index}/{total}" if isinstance(index, int) and isinstance(total, int) else None

        exec_time = data.get("execution_time")
        duration_s = round(exec_time, 2) if isinstance(exec_time, (int, float)) else None

        adapter = data.get("adapter_response")
        if isinstance(adapter, dict):
            rows_raw = adapter.get("rows_affected")
            rows_count = rows_raw if isinstance(rows_raw, int) else None
            msg_raw = adapter.get("_message")
            adapter_msg = msg_raw if isinstance(msg_raw, str) else None
        else:
            rows_count = None
            adapter_msg = None

        return _DbtResult(
            node_name=node_name,
            status=status,
            progress=progress,
            duration_s=duration_s,
            rows_count=rows_count,
            adapter_msg=adapter_msg,
        )

    @property
    def is_error(self) -> bool:
        """Whether the result represents a failed model/test."""
        return self.status.upper() in ("ERROR", "FAIL")

    def log(self, *, dbt_cmd: str) -> None:
        """Emit a structured log line for this result."""
        kwargs: dict[str, object] = {"dbt_cmd": dbt_cmd, "model": self.node_name}
        if self.progress is not None:
            kwargs["progress"] = self.progress
        if self.duration_s is not None:
            kwargs["duration_s"] = self.duration_s
        if self.rows_count is not None:
            kwargs["rows_count"] = self.rows_count
        if self.adapter_msg is not None:
            kwargs["adapter"] = self.adapter_msg

        log_fn = logger.error if self.is_error else logger.info
        log_fn("dbt result", status=self.status, **kwargs)


def _run_dbt(subcommand: Literal["run", "test"], extra_args: list[str] | None = None) -> None:
    """Run a dbt subcommand, streaming JSON log lines through structlog.

    Extracts rich data from dbt result events (Q007/Q012/Q034): model name, progress
    (index/total), execution time, and rows affected.
    """
    cmd = ["dbt", subcommand, *_DBT_COMMON_ARGS, *(extra_args or [])]
    logger.info("Starting dbt", dbt_cmd=subcommand)
    logger.debug("dbt full command", command=cmd)

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
    except OSError as exc:
        raise GoldStageError(
            f"Failed to start dbt {subcommand}: {exc}",
            dbt_cmd=subcommand,
        ) from None

    with proc:
        assert proc.stdout is not None  # guaranteed by PIPE
        for raw_line in proc.stdout:
            line = raw_line.rstrip()
            if not line:
                continue
            try:
                event = orjson.loads(line)
                meta = event.get("info", {})
                code = meta.get("code", "")

                # Rich logging for model/test results
                if code in _DBT_RESULT_CODES:
                    result = _DbtResult.from_raw(event.get("data", {}))
                    if result is None:
                        logger.warning(
                            "Malformed dbt result event",
                            dbt_cmd=subcommand,
                            dbt_code=code,
                        )
                        continue
                    result.log(dbt_cmd=subcommand)
                    continue

                msg = meta.get("msg", line)

                # Z017: visual separators in dbt output (empty lines)
                if not msg and code == "Z017":
                    continue

                level = meta.get("level", "info").lower()
                log_fn = _DBT_LOG_LEVELS.get(level, logger.info)
                log_fn(msg, dbt_cmd=subcommand, dbt_code=code or None)
            except orjson.JSONDecodeError:
                logger.debug(line, dbt_cmd=subcommand)

    if proc.returncode != 0:
        raise GoldStageError(
            f"dbt {subcommand} failed with exit code {proc.returncode}",
            dbt_cmd=subcommand,
            returncode=proc.returncode,
        )

    logger.info("dbt completed", dbt_cmd=subcommand)


# --------------------------------------------------------------------------------------
# DAG factory
# --------------------------------------------------------------------------------------


def _create_dag(
    dataset: GoldDatasetConfig,
    *,
    schedule: Asset | AssetAll,
    outlet: Asset,
) -> DAG:
    """Create a dbt DAG for a single gold dataset.

    Parameters
    ----------
    dataset
        Gold dataset configuration from the catalog.
    schedule
        Upstream silver PG Assets (``AssetAll`` or single ``Asset``) that trigger this
        DAG.
    outlet
        The gold Postgres Asset emitted after successful dbt test.

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
            _run_dbt("run", extra_args=["--select", f"+{dataset.name}"])

        @task(
            task_id=TASK_DBT_TEST,
            execution_timeout=TASK_DBT_TEST_TIMEOUT,
            outlets=[outlet],
        )
        def dbt_test() -> None:
            """Execute ``dbt test --select +{model}`` (sources + staging + gold)."""
            # '+' includes upstream sources + staging tests
            # (defense-in-depth: dbt re-validates post-load)
            _run_dbt("test", extra_args=["--select", f"+{dataset.name}"])

        # @task transforms return type to XComArg (DependencyMixin) at runtime
        chain(dbt_run(), dbt_test())  # type: ignore  # ty:ignore[unused-type-ignore-comment, unused-ignore-comment]

    return _dag()


def _generate_all_dags() -> dict[str, DAG]:
    """Generate dbt DAGs for all gold datasets in the catalog.

    Returns
    -------
    dict[str, DAG]
        Mapping of dataset names to their dbt DAG objects.
    """
    try:
        catalog = DataCatalog.load(settings.data_catalog_file_path)
    except InvalidCatalogError as e:
        e.log(logger.critical)
        return {}

    gold_datasets = catalog.get_gold_datasets()
    dags: dict[str, DAG] = {}

    for dataset in gold_datasets:
        try:
            gold_asset = get_gold_pg_asset(dataset)
            # GoldSourceConfig.validate_depends_on_not_empty enforces ≥1 dependency
            upstream_assets = [
                get_silver_pg_asset(catalog.get_remote_dataset(dep))
                for dep in dataset.source.depends_on
            ]
            if not upstream_assets:
                logger.error("No upstream assets found", dataset_name=dataset.name)
                continue
            schedule: Asset | AssetAll = (
                upstream_assets[0] if len(upstream_assets) == 1 else AssetAll(*upstream_assets)
            )
        except (ValueError, DataCatalogError):
            logger.exception("Invalid dataset configuration", dataset_name=dataset.name)
            continue  # move to next dataset

        dags[dataset.name] = _create_dag(dataset, schedule=schedule, outlet=gold_asset)
        logger.debug("to_gold DAG created", dataset_name=dataset.name)

    logger.info(
        "to_gold factory completed", created_count=len(dags), total_count=len(gold_datasets)
    )

    return dags


# Airflow discovers DAGs via @dag decorator; return value is intentionally unused.
_ = _generate_all_dags()
