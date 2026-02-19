"""Structured logging with environment-aware configuration.

Auto-detects the execution context (Airflow, TTY, or pipe) and applies
the most appropriate rendering strategy.  Exposes a single public
function — ``get_logger`` — that returns named structlog loggers.

Output modes
------------
airflow
    Extends Airflow 3's structlog processor chain with project-specific
    processors (see ``_setup_airflow_logger``).
tty
    Colored console output with timestamp, log level and logger name.
    Ideal for local interactive development.
plain
    Identical to *tty* but without colors — suitable when stderr is
    redirected to a file or piped to another process.
JSON
    Machine-readable JSON lines (UTC timestamps), serialized with
    *orjson* for speed.
"""

import re
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

OutputMode = Literal["airflow", "tty", "plain", "json"]


# ---------------------------------------------------------------------------
# Processors — normalize and flatten log values
# ---------------------------------------------------------------------------
_STRUCTLOG_INTERNAL_KEYS = frozenset({"event", "level", "timestamp", "_record", "_from_structlog"})


def _normalize_value(value: object) -> str | int | float | None:
    """Convert a value to a log-friendly primitive.

    Returns ``None`` to signal the key should be dropped.

    Notes
    -----
    Booleans are converted to ``"True"`` / ``"False"`` strings so that
    renderers relying on truthiness (e.g. Airflow's log display) do not
    silently swallow ``False``.  The ``bool`` branch must precede ``int``
    because ``bool`` is a subclass of ``int`` in Python.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, (str, int, float)):
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

    - Drops ``None`` values.
    - Converts ``Path`` and ``Enum`` to strings.
    - Flattens nested dicts: ``source={'provider': 'IGN'}``
      becomes ``source.provider=IGN``.
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


def _flatten_dict(prefix: str, mapping: EventDict) -> EventDict:
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
# Processors — console styling (tty / plain)
# ---------------------------------------------------------------------------
_RESET = "\033[0m"
_CYAN = "\033[36m"
_EVENT_PAD = 50
_ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*m")


def _visual_len(s: str) -> int:
    """Return the visual length of a string, ignoring ANSI escape codes."""
    return len(_ANSI_ESCAPE.sub("", s))


def _build_level_styles() -> dict[str, str]:
    """Build level styles from structlog defaults with project overrides.

    The returned dict is the single source of truth for level-based
    coloring: it is shared between ``ConsoleRenderer`` (level badge)
    and ``_colorize_event`` (event text).
    """
    styles = structlog.dev.ConsoleRenderer.get_default_level_styles(colors=True)
    styles["debug"] = "\033[2m"  # dim
    styles["info"] = ""  # default (white)
    return styles


def _colorize_event(
    level_styles: dict[str, str],
) -> structlog.types.Processor:
    """Return a processor that colorizes the event by level."""

    def processor(
        _logger: WrappedLogger,
        _method: str,
        event_dict: EventDict,
    ) -> EventDict:
        level: str = event_dict.get("level", "")
        color = level_styles.get(level)
        if color is not None and "event" in event_dict:
            event_dict["event"] = (
                f"{color}{event_dict['event']}{_RESET}" if color else event_dict["event"]
            )
        return event_dict

    return processor


def _prepend_logger_name(
    use_colors: bool = False,
    pad_to: int = 0,
) -> structlog.types.Processor:
    """Return a processor that prefixes the event with ``[logger_name]``.

    Inserted **after** ``_colorize_event`` so the name stays uncolored
    while the event text keeps its level color.  When *pad_to* is set,
    padding is applied to the full visual length of ``[name] event``
    (ANSI escape codes are excluded from the length calculation).
    """

    def processor(
        _logger: WrappedLogger,
        _method: str,
        event_dict: EventDict,
    ) -> EventDict:
        name = event_dict.pop("logger_name", None)
        prefix = (
            f"[{_CYAN}{name}{_RESET}]" if (name and use_colors) else (f"[{name}]" if name else "")
        )
        full = f"{prefix} {event_dict['event']}" if prefix else event_dict["event"]
        if pad_to:
            vlen = _visual_len(full)
            if vlen < pad_to:
                full += " " * (pad_to - vlen)
        event_dict["event"] = full
        return event_dict

    return processor


# ---------------------------------------------------------------------------
# Rich traceback helper
# ---------------------------------------------------------------------------
def _rich_traceback(
    use_colors: bool = True,
    width: int | None = None,
    show_locals: bool = False,
    word_wrap: bool = False,
) -> structlog.dev.RichTracebackFormatter:
    """Create a ``RichTracebackFormatter`` with customized rendering."""
    return structlog.dev.RichTracebackFormatter(
        width=width,
        show_locals=show_locals,
        word_wrap=word_wrap,
        color_system="truecolor" if use_colors else "auto",
    )


# ---------------------------------------------------------------------------
# Environment detection
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
# Airflow-specific configuration
# ---------------------------------------------------------------------------
def _setup_airflow_logger() -> None:
    """Extend Airflow 3's structlog config with project processors.

    Airflow 3 already calls ``structlog.configure()`` at startup with
    its own processor chain.  Instead of replacing it, we read the
    existing chain and insert our processors before the final renderer.

    Currently, injected processors:

    - ``_normalize_and_flatten``

    Notes
    -----
    Two levels of customization are possible:

    1. **Add a processor** (current approach)::

        processors = list(structlog.get_config()["processors"])
        processors.insert(-1, my_processor)
        structlog.configure(processors=processors)

    2. **Replace a processor** (e.g. ``TimeStamper``)::

        processors = [
            p for p in config["processors"]
            if not isinstance(p, structlog.processors.TimeStamper)
        ]
        processors.insert(-1, custom_timestamper)
        structlog.configure(processors=processors)

    Replacing Airflow processors may break the task log UI if the
    replacement omits keys expected by Airflow's renderer (e.g.
    ``'timestamp'`` for ``json_processor``).
    """
    config = structlog.get_config()
    processors = list(config.get("processors", []))
    # insert(-1) places our processor just before the final renderer
    processors.insert(-1, _normalize_and_flatten)
    structlog.configure(processors=processors)


# ---------------------------------------------------------------------------
# Global configuration (called once at first import)
# ---------------------------------------------------------------------------
def _setup_logger(
    output: OutputMode | None = None,
    level: LogLevel = settings.logging_level,
) -> None:
    """Configure structlog globally.

    Parameters
    ----------
    output
        Logging output mode. ``None`` triggers auto-detection.
    level
        Minimum severity level.
    """
    if output is None:
        output = _detect_output_mode()

    # --- Airflow delegates to its own config ---
    if output == "airflow":
        _setup_airflow_logger()
        return

    # --- Shared processors (tty / plain / json) ---
    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S %Z", utc=False),
    ]

    # --- Console modes (tty / plain) ---
    if output in ("tty", "plain"):
        use_colors = output == "tty"
        level_styles = _build_level_styles() if use_colors else None

        console_chain: list[structlog.types.Processor] = [_normalize_and_flatten]
        if use_colors and level_styles:
            console_chain.append(_colorize_event(level_styles))
        console_chain.append(
            _prepend_logger_name(use_colors, pad_to=_EVENT_PAD if use_colors else 0)
        )
        console_chain.append(
            structlog.dev.ConsoleRenderer(
                colors=use_colors,
                sort_keys=False,
                level_styles=level_styles,
                pad_event_to=0 if use_colors else _EVENT_PAD,
                exception_formatter=_rich_traceback(
                    use_colors=use_colors,
                    show_locals=use_colors,
                    word_wrap=use_colors,
                ),
            ),
        )
        processors = shared_processors + console_chain
        logger_factory = structlog.PrintLoggerFactory(file=sys.stderr)

    # --- JSON mode ---
    elif output == "json":
        processors = shared_processors + [
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(serializer=orjson.dumps),
        ]
        logger_factory = structlog.BytesLoggerFactory()

    else:
        msg = f"Unknown output mode: {output!r}"
        raise ValueError(msg)

    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(min_level=level),
        processors=processors,
        logger_factory=logger_factory,
        cache_logger_on_first_use=True,
    )


# ---------------------------------------------------------------------------
# Module initialization
# ---------------------------------------------------------------------------
_setup_logger()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def get_logger(name: str | None = None) -> BoundLogger:
    """Return a named structlog logger.

    Parameters
    ----------
    name
        Logger name.  Appears as ``[name]`` in console output.
        ``None`` returns an unnamed default logger.

    Returns
    -------
    BoundLogger
        A bound logger instance.
    """
    if name is None:
        return structlog.get_logger()

    # TODO: auto-shorten dotted module paths to allow __name__ usage
    # short = name.rsplit(".", maxsplit=1)[-1]

    return structlog.get_logger(logger_name=name)


# ---------------------------------------------------------------------------
# Manual smoke test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    from pydantic import BaseModel

    class X(BaseModel):
        """Dummy class used for logging demonstration."""

        x: float
        y: float

    _x = X(x=1, y=2)
    _logger = get_logger("demo")

    _logger.debug("debug text", extra_data={"k": 3}, x=_x)
    _logger.info("info text", extra_data={"k": 3}, x=_x)
    _logger.warning("warning text", extra_data={"k": 3}, x=_x)
    _logger.error("error text", extra_data={"k": 3}, x=_x)
    _logger.critical("critical text", extra_data={"k": 3}, x=_x)

    _logger.exception("exception text (without)", extra_data={"k": 3}, x=_x)
    try:
        _ = 1 / 0
    except ZeroDivisionError:
        _logger.exception("exception text (with)", extra_data={"k": 3}, x=_x)
