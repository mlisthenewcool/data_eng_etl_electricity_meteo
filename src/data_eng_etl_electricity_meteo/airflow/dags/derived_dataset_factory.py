"""Smoke-test DAG for verifying structlog integration in Airflow."""

from airflow.sdk import DAG, Asset, XComArg, dag, task

from data_eng_etl_electricity_meteo.airflow.assets import get_asset
from data_eng_etl_electricity_meteo.core.logger import get_logger

logger = get_logger("dag_factory")

__all__: list[str] = []


def _create_dag(asset: Asset) -> DAG:
    @dag(
        dag_id="test_logging_dag",
    )
    def _dag() -> None:
        @task(outlets=[asset])
        def airflow_logger_task() -> XComArg:
            from pydantic import BaseModel  # noqa: PLC0415

            class X(BaseModel):
                x: float
                y: float

            _x = X(x=1, y=2)

            logger.debug("debug text", extra_data={"k": 3}, x=_x)
            logger.info("info text", extra_data={"k": 3}, x=_x)
            logger.warning("warning text", extra_data={"k": 3}, x=_x)
            logger.error("error text", extra_data={"k": 3}, x=_x)
            logger.critical("critical text", extra_data={"k": 3}, x=_x)

            logger.exception("exception text (without)", extra_data={"k": 3}, x=_x)
            try:
                _ = 1 / 0
            except ZeroDivisionError:
                logger.exception("exception text (with)", extra_data={"k": 3}, x=_x)

            return _x.model_dump(mode="json")

        airflow_logger_task()

    return _dag()


def _generate_all_dags() -> dict[str, DAG]:
    return {
        "ign_contours_iris": _create_dag(get_asset("ign_contours_iris", "silver")),
    }


_generate_all_dags()
