import marimo

__generated_with = "0.19.9"
app = marimo.App(width="full")


@app.cell
def _():
    from pathlib import Path  # noqa: PLC0415

    from data_eng_etl_electricity_meteo.core.logger import logger  # noqa: PLC0415

    return Path, logger


@app.cell
def _(Path, logger):
    extras = {"status": "working", "user_id": 42}

    logger.debug("Debug message", extra=extras)
    logger.info("Info message", extra=extras)
    logger.warning("Warning message", extra=extras)
    logger.error("Error message", extra=extras)
    logger.critical("Critical message", extra=extras)

    logger.info("Message without extras")

    # Thanks to `_safe_str(...)`, objects with __str__ method implemented use it automatically
    path = Path(__file__).name
    logger.info(f"Message with path {path}", extra={"path": path})

    # ANSI injection test - codes should be stripped
    logger.info("ANSI test", extra={"\x1b[1;31mred as key": "\033[1;31mred as value"})

    logger.exception("No active exception")

    logger.info("After exception")


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
