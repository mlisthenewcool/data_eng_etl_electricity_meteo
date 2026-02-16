"""Structured logging module providing environment-aware configuration.

The module auto-detects the execution context (Airflow, TTY, or Pipe)
to apply the most appropriate rendering and processing strategy.

Notes
-----
This setup implements four specific output modes:

airflow:
    Delegates entirely to Airflow's 3+ internal structlog config.
tty:
    colored console output with timestamp and log level, ideal for local
    interactive development.
plain:
    identical to *tty* but without colors — suitable when stderr is redirected
    to a file or piped to another process.
json:
    machine-readable JSON lines (UTC timestamps), serialized with *orjson* for
    speed.
"""

import logging
import sys
from enum import Enum
from functools import cache
from pathlib import Path
from typing import Literal

import orjson
import structlog
from structlog.stdlib import BoundLogger
from structlog.typing import EventDict, WrappedLogger

from data_eng_etl_electricity_meteo.core.settings import LogLevel, settings

__all__: list[str] = ["logger"]

OutputMode = Literal["airflow", "tty", "plain", "json"]


# ---------------------------------------------------------------------------
# Normalize and flatten log values
# ---------------------------------------------------------------------------
_STRUCTLOG_INTERNAL_KEYS = frozenset({"event", "level", "timestamp", "_record", "_from_structlog"})


def _normalize_value(value: object) -> str | int | float | bool | None:
    """Convert a value to a log-friendly primitive.

    Returns ``None`` to signal the key should be dropped.
    """
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Enum):
        return _normalize_value(value.value)
    return repr(value)


def _normalize_and_flatten(
    _logger: WrappedLogger,
    _method: str,
    event_dict: EventDict,
) -> EventDict:
    """Normalize values and flatten nested dicts into dotted keys.

    *source={'provider': 'IGN', 'format': '7z'}*
    becomes *source.provider=IGN  source.format=7z*.

    Drops ``None`` values and converts Path/Enum to strings.
    """
    flat: EventDict = {}
    for key, value in event_dict.items():
        if key in _STRUCTLOG_INTERNAL_KEYS:
            flat[key] = value
        elif isinstance(value, dict):
            flat |= _flatten_dict(key, value)
        else:
            normalized = _normalize_value(value)
            if normalized is not None:
                flat[key] = normalized
    return flat


def _flatten_dict(
    prefix: str,
    mapping: EventDict,
) -> EventDict:
    """Recursively flatten *mapping* into dotted keys."""
    result: EventDict = {}
    for key, value in mapping.items():
        dotted = f"{prefix}.{key}"
        if isinstance(value, dict):
            result |= _flatten_dict(dotted, value)
        else:
            normalized = _normalize_value(value)
            if normalized is not None:
                result[dotted] = normalized
    return result


# ---------------------------------------------------------------------------
# Environment detection (evaluated once at import time)
# ---------------------------------------------------------------------------
@cache
def _detect_output_mode() -> OutputMode:
    """Choose the best output mode for the current environment."""
    if settings.is_running_on_airflow:
        return "airflow"
    if sys.stderr.isatty():
        return "tty"
    return "plain"


# ---------------------------------------------------------------------------
# Colorize event text by log level
# ---------------------------------------------------------------------------
_RESET = "\033[0m"
_LEVEL_COLORS: dict[str, str] = {
    LogLevel.DEBUG: "\033[2m",  # dim
    LogLevel.INFO: "\033[32m",  # green
    LogLevel.WARNING: "\033[33m",  # yellow
    LogLevel.ERROR: "\033[31m",  # red
    LogLevel.CRITICAL: "\033[1;31m",  # bold red
}

_EVENT_PAD = 30


def _colorize_event(
    _logger: WrappedLogger,
    _method: str,
    event_dict: EventDict,
) -> EventDict:
    """Pad the event to ``_EVENT_PAD`` visible chars, then colorize."""
    level: str = event_dict.get("level", "").upper()
    color = _LEVEL_COLORS.get(level)
    if color and "event" in event_dict:
        event = f"{event_dict['event']:<{_EVENT_PAD}}"
        event_dict["event"] = f"{color}{event}{_RESET}"
    return event_dict


# ---------------------------------------------------------------------------
# Rich traceback helper
# ---------------------------------------------------------------------------
def _rich_traceback(
    use_colors: bool = True,
    width: int | None = None,
    show_locals: bool = False,
    word_wrap: bool = False,
) -> structlog.dev.RichTracebackFormatter:
    """Create a RichTracebackFormatter with customized rendering."""
    return structlog.dev.RichTracebackFormatter(
        width=width,
        show_locals=show_locals,
        word_wrap=word_wrap,
        color_system="truecolor" if use_colors else "auto",
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def setup_logger(
    output: OutputMode | None = None,
    level: LogLevel = settings.logging_level,
) -> BoundLogger:
    """Configure *structlog* globally and return a bound logger.

    Parameters
    ----------
    output
        Logging output mode. ``None`` triggers auto-detection.
    level
        Minimum severity level as a string.

    Returns
    -------
    BoundLogger
        A ready-to-use bound logger instance.

    Notes
    -----
    In 'airflow' mode, we strictly avoid calling ``structlog.configure()``
    to prevent overwriting Airflow 3's internal logging setup, which
    would break the Task UI integration. Airflow 3 already configures structlog
    globally. Calling ``structlog.configure()`` here would overwrite its state and
    break Airflow's own logging.
    By simply returning a logger bound to the project name, logs will flow
    through Airflow's handlers (task UI, file logs, remote storage) while
    retaining native features like : timestamp, level, JWT redaction ...

    For full rendering control (at the cost of Airflow UI integration)
    see ``_setup_airflow_bypass_logger()``.
    """
    if output is None:
        output = _detect_output_mode()

    # --- processors shared by every mode ---
    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S %Z", utc=False),
    ]

    # --- mode-specific configuration ---
    if output == "airflow":
        return structlog.get_logger(logger_name=settings.logger_name)  # or "airflow.task"

    if output in ("tty", "plain"):
        use_colors = output == "tty"
        console_chain: list[structlog.types.Processor] = [_normalize_and_flatten]
        if use_colors:
            console_chain.append(_colorize_event)
        console_chain.append(
            structlog.dev.ConsoleRenderer(
                colors=use_colors,
                pad_event_to=0 if use_colors else _EVENT_PAD,
                exception_formatter=_rich_traceback(
                    use_colors=use_colors,
                    show_locals=use_colors,
                    word_wrap=use_colors,
                ),
            ),
        )
        processors: list[structlog.types.Processor] = shared_processors + console_chain
        logger_factory = structlog.PrintLoggerFactory(file=sys.stderr)

    elif output == "json":
        processors = shared_processors + [
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(serializer=orjson.dumps),
        ]
        logger_factory = structlog.BytesLoggerFactory()

    else:
        # Should never happen thanks to the Literal type, but just in case.
        msg = f"Unknown output mode: {output!r}"
        raise ValueError(msg)

    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(min_level=level),
        processors=processors,
        logger_factory=logger_factory,
        # Note: the cache freezes each logger after its first use.
        # A subsequent call to setup_logger() with other parameters
        # will NOT update loggers that have already been cached.
        cache_logger_on_first_use=True,
    )

    return structlog.get_logger()


# ---------------------------------------------------------------------------
# Airflow bypass (kept for reference — not used by default)
# ---------------------------------------------------------------------------
def _setup_airflow_bypass_logger(
    level: LogLevel,
    shared_processors: list[structlog.types.Processor],
) -> BoundLogger:
    """Bypass Airflow's structlog config — writes directly to stderr.

    Parameters
    ----------
    level:
        Minimum severity level as a string.
    shared_processors:
        The common processor chain to build upon.

    Returns
    -------
    BoundLogger
        A logger instance mapped to a stdlib StreamHandler.

    Notes
    -----
    * Not used by default: Kept here in case we need full control over rendering
    (rich traceback width, colors, timestamp format, etc.) at the cost of losing
    Airflow UI log integration and remote logging.

    * (warning) Logs bypass Airflow's handlers → not visible in the task UI or
    remote storage (S3, GCS, etc.).  They only appear on stderr.

    * (warning) Calling ``structlog.configure()`` overwrites Airflow's global
    config, which can break Airflow's own internal logging.

    """
    processors: list[structlog.types.Processor] = shared_processors + [
        structlog.stdlib.add_logger_name,
        structlog.stdlib.ExtraAdder(),
        structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
    ]

    formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.dev.ConsoleRenderer(
                colors=True,
                exception_formatter=_rich_traceback(use_colors=True),
            ),
        ],
    )

    # Attach the formatter to a named stdlib logger.
    stdlib_logger = logging.getLogger(settings.logger_name)
    stdlib_logger.handlers.clear()
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(formatter)
    stdlib_logger.addHandler(handler)
    stdlib_logger.setLevel(level)

    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(min_level=level),
        processors=processors,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    return structlog.stdlib.get_logger(logger_name=settings.logger_name)


# ---------------------------------------------------------------------------
# Module-level singleton (auto-detected mode if no ``output`` parameter)
# ---------------------------------------------------------------------------
logger = setup_logger()
"""The singleton logger instance."""


if __name__ == "__main__":
    from pydantic import BaseModel

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
