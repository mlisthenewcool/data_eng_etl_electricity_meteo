import sys

import orjson
import structlog
from pydantic import BaseModel

from data_eng_etl_electricity_meteo.core.settings import settings


class X(BaseModel):
    x: float
    y: float


_x = X(x=1, y=2)


def multi_loggers():
    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S %Z", utc=False),
    ]

    console_log = structlog.wrap_logger(
        None,
        cache_logger_on_first_use=True,
        wrapper_class=structlog.make_filtering_bound_logger(settings.logging_level),
        logger_factory=structlog.PrintLoggerFactory(sys.stderr),
        processors=shared_processors
        + [
            structlog.dev.ConsoleRenderer(
                colors=True,
                exception_formatter=structlog.dev.RichTracebackFormatter(
                    width=None, show_locals=True, color_system="standard"
                ),
            ),
        ],
    )

    json_log = structlog.wrap_logger(
        None,
        cache_logger_on_first_use=True,
        wrapper_class=structlog.make_filtering_bound_logger(settings.logging_level),
        logger_factory=structlog.BytesLoggerFactory(),
        processors=shared_processors
        + [
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(serializer=orjson.dumps),
        ],
    )

    return console_log, json_log


def test_output(logger: structlog.stdlib.BoundLogger) -> None:
    logger.debug("debug text <red> \033[0;33m ezeze", ez={"ezez": 3}, _x=_x)
    logger.info("info text", ez={"ezez": 3}, _x=_x)
    logger.warning("warning text", ez={"ezez": 3}, _x=_x)
    logger.error("error text", ez={"ezez": 3}, _x=_x)
    logger.critical("critical text", ez={"ezez": 3}, _x=_x)

    logger.exception("exception text (no active exception)", ez={"ezez": 3}, _x=_x)
    try:
        _ = 1 / 0
    except ZeroDivisionError:
        logger.exception("exception text (with active exception)", ez={"ezez": 3}, _x=_x)


if __name__ == "__main__":
    _console_logger, _json_logger = multi_loggers()

    print("=" * 80)
    print("CONSOLE LOGGER")
    print("=" * 80)
    test_output(_console_logger)

    print("=" * 80)
    print("JSON LOGGER")
    print("=" * 80)
    test_output(_json_logger)
