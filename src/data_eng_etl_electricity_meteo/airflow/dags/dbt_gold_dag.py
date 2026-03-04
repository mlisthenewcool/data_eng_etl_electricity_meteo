"""DAG for dbt gold transformations (silver PG -> gold PG).

Runs ``dbt run`` then ``dbt test`` inside the Airflow container.
Triggered when all three required silver PG tables have been updated.

Pipeline position::

    [*_load_pg]  ->  silver PG Assets  ->  [dbt_gold]  ->  gold PG Asset
       LOAD                                   ELT
"""

import subprocess
from datetime import timedelta
from pathlib import Path

import orjson
from airflow.sdk import AssetAll, dag, task

from data_eng_etl_electricity_meteo.airflow.assets import get_gold_pg_asset, get_silver_pg_asset
from data_eng_etl_electricity_meteo.airflow.defaults import DEFAULT_ARGS, START_DATE
from data_eng_etl_electricity_meteo.core.logger import get_logger

logger = get_logger("dag.dbt_gold")

DBT_PROJECT_DIR = Path("/opt/airflow/dbt")
DBT_PROFILES_DIR = Path("/opt/airflow/dbt")
DBT_TARGET = "docker"
# dbt project dir is mounted read-only; redirect writable artifacts elsewhere
DBT_LOG_PATH = Path("/tmp/dbt/logs")
DBT_TARGET_PATH = Path("/tmp/dbt/target")

_DBT_COMMON_ARGS = [
    "--project-dir",
    str(DBT_PROJECT_DIR),
    "--profiles-dir",
    str(DBT_PROFILES_DIR),
    "--target",
    DBT_TARGET,
    "--log-path",
    str(DBT_LOG_PATH),
    "--target-path",
    str(DBT_TARGET_PATH),
    "--log-format",
    "json",
]

GOLD_ASSET = get_gold_pg_asset("installations_renouvelables_avec_stations_meteo")

SCHEDULE = AssetAll(
    get_silver_pg_asset("odre_installations"),
    get_silver_pg_asset("meteo_france_stations"),
    get_silver_pg_asset("ign_contours_iris"),
)

_DBT_LOG_LEVELS = {
    "debug": logger.debug,
    "info": logger.info,
    "warn": logger.warning,
    "warning": logger.warning,
    "error": logger.error,
}


def _run_dbt(subcommand: str, extra_args: list[str] | None = None) -> None:
    """Run a dbt subcommand, streaming JSON log lines through structlog."""
    cmd = ["dbt", subcommand, *_DBT_COMMON_ARGS, *(extra_args or [])]
    logger.info("Starting dbt", command=" ".join(cmd))

    with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True) as proc:
        assert proc.stdout is not None  # guaranteed by PIPE
        for raw_line in proc.stdout:
            line = raw_line.rstrip()
            if not line:
                continue
            try:
                event = orjson.loads(line)
                info = event.get("info", {})
                msg = info.get("msg", line)
                level = info.get("level", "info").lower()
                log_fn = _DBT_LOG_LEVELS.get(level, logger.info)
                log_fn(msg, dbt_cmd=subcommand, dbt_code=info.get("code"))
            except orjson.JSONDecodeError:
                logger.info(line, dbt_cmd=subcommand)

    if proc.returncode != 0:
        raise subprocess.CalledProcessError(proc.returncode, cmd)

    logger.info("dbt finished successfully", dbt_cmd=subcommand)


@dag(
    dag_id="dbt_gold",
    schedule=SCHEDULE,
    start_date=START_DATE,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["dbt", "gold", "postgres"],
    doc_md=__doc__,
)
def dbt_gold() -> None:
    """Run dbt models and tests for the gold layer."""

    @task(
        task_id="dbt_run",
        execution_timeout=timedelta(minutes=15),
    )
    def dbt_run() -> None:
        """Execute ``dbt run --select +gold``."""
        # '+' includes upstream silver staging views
        _run_dbt("run", ["--select", "+gold"])

    @task(
        task_id="dbt_test",
        execution_timeout=timedelta(minutes=10),
        outlets=[GOLD_ASSET],
    )
    def dbt_test() -> None:
        """Execute ``dbt test`` on all models."""
        _run_dbt("test")

    dbt_run() >> dbt_test()


dbt_gold()
