from airflow import XComArg
from airflow.sdk import DAG, Asset, dag, task
from pydantic import BaseModel

from data_eng_etl_electricity_meteo.airflow.assets import ASSETS
from data_eng_etl_electricity_meteo.main_structlog import multi_loggers, test_output


class X(BaseModel):
    x: float
    y: float


_x = X(x=1, y=2)


def _create_dag(asset: Asset) -> DAG:
    @dag(
        dag_id="landing_to_silver_factory",
    )
    def _dag() -> None:
        @task(outlets=[asset])
        def console_logger_task() -> XComArg:
            console_loger, _ = multi_loggers()
            test_output(console_loger)

            return _x.model_dump(mode="json")

        @task
        def json_logger_task() -> XComArg:
            _, json_logger = multi_loggers()
            test_output(json_logger)

            return _x.model_dump(mode="json")

        console_logger_task()
        json_logger_task()

    return _dag()


def _generate_all_dags() -> dict[str, DAG]:
    return {
        "ign_contours_iris": _create_dag(ASSETS["ign_contours_iris"]),
    }


_generate_all_dags()
