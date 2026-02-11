from airflow import XComArg
from airflow.sdk import DAG, Asset, dag, task
from pydantic import BaseModel

from data_eng_etl_electricity_meteo.airflow.assets import ASSETS
from data_eng_etl_electricity_meteo.core.logger import logger


class M(BaseModel):
    success: bool


def _create_dag(asset: Asset) -> DAG:
    @dag(
        dag_id="landing_to_silver_factory",
    )
    def _dag() -> None:
        @task(outlets=[asset])
        def first_task() -> XComArg:
            logger.debug(
                "debug msg",
                extra={"pour voir": "OK", "ez": "ezez", "ezezezez": "ezezezez", "23": 392323},
            )
            logger.info("info msg", extra={"pour voir": "OK"})
            logger.warning("warning msg", extra={"pour voir": "OK"})
            logger.exception("exception msg", extra={"pour voir nested": {"0": 123, "1": 456}})
            m = M(success=True)
            return m.model_dump(mode="json")

        first_task()

    return _dag()


def _generate_all_dags() -> dict[str, DAG]:
    return {
        "ign_contours_iris": _create_dag(ASSETS["ign_contours_iris"]),
    }


_generate_all_dags()
