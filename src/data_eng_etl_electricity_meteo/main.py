from data_eng_etl_electricity_meteo.core.logger import logger


def test() -> None:
    logger.debug("debug test", extra={"context": "fonctionne !"})
    logger.info("info test", extra={"context": "fonctionne !"})


if __name__ == "__main__":
    test()
