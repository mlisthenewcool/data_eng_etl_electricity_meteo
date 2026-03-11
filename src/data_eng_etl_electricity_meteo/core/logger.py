"""Structured logging with environment-aware configuration.

Auto-detects the execution context (Airflow, TTY, or pipe) and applies the most
appropriate rendering strategy.  Exposes a single public function — ``get_logger`` —
that returns named structlog loggers.

Output modes
------------
airflow
    Extends Airflow 3's structlog processor chain with project-specific processors
    (see ``_setup_airflow_logger``).
tty
    Colored console output with timestamp, log level and logger name.
    Ideal for local interactive development.
plain
    Identical to ``tty`` but without colors — suitable when stderr is redirected to a
    file or piped to another process.
json
    Machine-readable JSON lines (UTC timestamps), serialized with ``orjson`` for speed.
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


# --------------------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------------------


_RESET = "\033[0m"
_DIM = "\033[2m"
_CYAN = "\033[36m"
_ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*m")

_EVENT_PAD = 60

# Structlog-managed keys that must be preserved as-is by normalization processors.
_STRUCTLOG_INTERNAL_KEYS = frozenset({"event", "level", "timestamp", "_record", "_from_structlog"})


# --------------------------------------------------------------------------------------
# Shared helper — scalar value normalization
# --------------------------------------------------------------------------------------


def _normalize_value(value: object) -> str | int | float:
    """Convert a non-``None`` value to a log-friendly primitive.

    ``None`` inputs are not accepted — callers handle ``None`` explicitly before calling
    this function (drop it or preserve it as a JSON null).

    Notes
    -----
    Booleans are converted to ``"True"`` / ``"False"`` strings so that renderers relying
    on truthiness (e.g. Airflow's log display) do not silently swallow ``False``.
    The ``bool`` branch must precede ``int`` because ``bool`` is a subclass of ``int``
    in Python.
    """
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, (str, int, float)):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Enum):
        return _normalize_value(value.value)
    return repr(value)


# --------------------------------------------------------------------------------------
# Shared processors (console + Airflow)
# --------------------------------------------------------------------------------------


def _visual_len(s: str) -> int:
    """Return the printable length of *s*, excluding ANSI escape codes."""
    return len(_ANSI_ESCAPE.sub("", s))


def _pad_event(pad_to: int) -> structlog.types.Processor:
    """Return a processor that right-pads the event string with dots.

    Used in both console (tty) and Airflow modes for visual alignment.
    ANSI escape codes are excluded from the length calculation.

    Must be inserted **after** ``_prepend_logger_name`` in console chains so that the
    padding accounts for the ``[name] `` prefix length.  In the Airflow chain
    (where no logger-name prefix exists), it pads the bare event message.

    Parameters
    ----------
    pad_to
        Target visual width.  Events already at or above this width are left unchanged.
    """

    def processor(_logger: WrappedLogger, _method_name: str, event_dict: EventDict) -> EventDict:
        visual_len = _visual_len(event_dict["event"])
        if visual_len < pad_to:
            event_dict["event"] += "." * (pad_to - visual_len)
        return event_dict

    return processor


# --------------------------------------------------------------------------------------
# Console processors (tty / plain)
# --------------------------------------------------------------------------------------


def _flatten_dict(prefix: str, mapping: EventDict) -> EventDict:
    """Recursively flatten *mapping* into dotted-key entries.

    When *prefix* is empty, top-level keys are used as-is; otherwise each key is
    prepended with *prefix* and a dot. ``None`` leaves are dropped. Scalar values are
    returned as-is — normalization is the caller's responsibility.
    """
    result: EventDict = {}
    for key, value in mapping.items():
        dotted = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            result |= _flatten_dict(dotted, mapping=value)
        elif value is not None:
            result[dotted] = value
    return result


def _flatten_and_normalize(
    _logger: WrappedLogger,
    _method_name: str,
    event_dict: EventDict,
) -> EventDict:
    """Flatten nested dicts into dotted keys and normalize scalar values.

    Console modes (tty / plain) only — **not** for Airflow, which requires nested dict
    structures to be preserved (see ``_normalize_types``).

    - Structlog internal keys (``event``, ``level``, etc.) are passed through as-is.
    - Nested dicts are flattened: ``{'source': {'provider': 'IGN'}}`` becomes
      ``source.provider=IGN``.
    - ``None`` values are dropped; ``Path``, ``Enum``, and ``bool`` are converted to
      strings.
    """
    internal: EventDict = {}
    external: EventDict = {}
    for k, v in event_dict.items():
        (internal if k in _STRUCTLOG_INTERNAL_KEYS else external)[k] = v
    flattened = _flatten_dict("", mapping=external)
    return {**internal, **{k: _normalize_value(v) for k, v in flattened.items()}}


def _build_level_styles() -> dict[str, str]:
    """Build the level → ANSI color mapping for console rendering.

    Returns structlog's default styles with project overrides.  The returned dict is the
    single source of truth for level-based coloring: it is shared between
    ``ConsoleRenderer`` (level badge) and ``_colorize_event`` (event text).

    Returns
    -------
    dict[str, str]
        Mapping of lowercase level names to ANSI escape sequences.
        An empty string means "no color" (inherits terminal default).
    """
    styles = structlog.dev.ConsoleRenderer.get_default_level_styles(colors=True)
    styles["debug"] = _DIM
    styles["info"] = ""  # no color — inherits terminal default (white)
    return styles


def _colorize_event(
    level_styles: dict[str, str],
) -> structlog.types.Processor:
    """Return a processor that wraps the event string with the level's ANSI color.

    Levels absent from *level_styles* or whose style is an empty string
    (e.g. ``"info"``) are left uncolored.

    Parameters
    ----------
    level_styles
        Mapping of lowercase level names to ANSI escape sequences, as returned by
        ``_build_level_styles``.
    """

    def processor(_logger: WrappedLogger, _method_name: str, event_dict: EventDict) -> EventDict:
        color = level_styles.get(event_dict.get("level", ""))
        if color:
            event_dict["event"] = f"{color}{event_dict['event']}{_RESET}"
        return event_dict

    return processor


def _prepend_logger_name(
    *,
    use_colors: bool = False,
) -> structlog.types.Processor:
    """Return a processor that prefixes the event with ``[logger_name]``.

    Must be inserted **after** ``_colorize_event`` so the name stays uncolored while the
    event text keeps its level color, and **before** ``_pad_event`` so that the padding
    includes the prefix length.

    Parameters
    ----------
    use_colors
        Render the logger name in cyan when ``True``.
    """

    def processor(_logger: WrappedLogger, _method_name: str, event_dict: EventDict) -> EventDict:
        name = event_dict.pop("logger_name", None)
        prefix = (
            f"[{_CYAN}{name}{_RESET}]" if (name and use_colors) else (f"[{name}]" if name else "")
        )
        if prefix:
            event_dict["event"] = f"{prefix} {event_dict['event']}"
        return event_dict

    return processor


def _rich_traceback(
    *,
    use_colors: bool = True,
    width: int | None = None,
    show_locals: bool = False,
    word_wrap: bool = False,
) -> structlog.dev.RichTracebackFormatter:
    """Create a ``RichTracebackFormatter`` with project-specific defaults.

    Parameters
    ----------
    use_colors
        Use truecolor ANSI output.  ``False`` falls back to auto-detection.
    width
        Maximum render width in characters.  ``None`` uses the terminal width.
    show_locals
        Include local variables in each stack frame.
    word_wrap
        Wrap long lines within the traceback.
    """
    return structlog.dev.RichTracebackFormatter(
        width=width,
        show_locals=show_locals,
        word_wrap=word_wrap,
        color_system="truecolor" if use_colors else "auto",
    )


# --------------------------------------------------------------------------------------
# Airflow processors
# --------------------------------------------------------------------------------------


def _walk(value: object) -> object:
    """Recursively convert non-JSON-serializable leaves, preserving container structure.

    ``None`` is returned as-is and stored unconditionally by the caller
    (``_normalize_types``), preserving JSON nulls.

    Containers are recursed into rather than repr'd, because Airflow's UI expects them
    intact (e.g. ``ExceptionDictTransformer`` output). Tuples are converted to lists —
    JSON has no tuple type and both serialize identically.
    """
    if value is None:
        return None
    if isinstance(value, dict):
        return {k: _walk(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_walk(v) for v in value]
    return _normalize_value(value)


def _normalize_types(
    _logger: WrappedLogger,
    _method_name: str,
    event_dict: EventDict,
) -> EventDict:
    """Lightly normalize scalar types for Airflow's JSON log pipeline.

    Unlike ``_flatten_and_normalize``, this processor preserves the full dict/list
    structure and ``None`` values — Airflow's ``ExceptionDictTransformer`` produces
    nested structures (``[{"type": …, "frames": […]}]``) that the UI expects intact.
    Only scalar leaves (``Path``, ``Enum``, ``bool``) are converted to JSON-serializable
    primitives.
    """
    return {key: _walk(value) for key, value in event_dict.items()}


def _setup_airflow_logger() -> None:
    """Insert project processors into Airflow 3's existing structlog chain.

    Airflow 3 task subprocesses send structured JSON logs to the supervisor via a
    dedicated pipe (``NamedBytesLogger``).
    The supervisor writes them to the log file; the UI renders that JSON.

    The existing chain must **not** be flattened — Airflow's
    ``ExceptionDictTransformer`` produces nested structures
    (``[{"type": …, "frames": […]}]``) that the UI expects intact.

    Injected processors (both inserted before the final renderer):

    1. ``_normalize_types`` — scalar type conversion (``Path`` → ``str``, ``Enum`` →
       value).
    2. ``_pad_event`` — right-pads the event message with dots for visual alignment,
       matching the console (tty) rendering style.

    This function is idempotent: calling it multiple times leaves the processor chain
    unchanged after the first insertion.
    """
    config = structlog.get_config()
    processors = list(config.get("processors", []))
    if processors and _normalize_types not in processors:
        processors.insert(-1, _normalize_types)  # insert before the final renderer
        processors.insert(-1, _pad_event(_EVENT_PAD))
        structlog.configure(processors=processors)


# --------------------------------------------------------------------------------------
# Environment detection
# --------------------------------------------------------------------------------------


@cache
def _detect_output_mode() -> OutputMode:
    """Infer the best output mode for the current execution environment.

    Result is cached after the first call — reconfiguring structlog at runtime does not
    re-trigger detection.

    Notes
    -----
    ``"json"`` mode is never auto-detected; it must be explicitly requested via
    ``_setup_logger(output="json")``.
    """
    if settings.is_running_on_airflow:
        return "airflow"
    if sys.stderr.isatty():
        return "tty"
    return "plain"


# --------------------------------------------------------------------------------------
# Global configuration
# --------------------------------------------------------------------------------------


def _setup_logger(
    output: OutputMode | None = None,
    level: LogLevel = settings.logging_level,
) -> None:
    """Configure structlog globally.

    Called once at module import.
    Can be called again to reconfigure structlog (e.g. in tests).

    Parameters
    ----------
    output
        Output mode.  ``None`` triggers auto-detection via ``_detect_output_mode``.
    level
        Minimum severity level.
        Defaults to ``settings.logging_level`` evaluated at import time.
    """
    if output is None:
        output = _detect_output_mode()

    # Airflow already has its own structlog chain; only inject our processors.
    if output == "airflow":
        _setup_airflow_logger()
        return

    # -- Build shared processor chain for all non-Airflow modes ------------------------

    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.StackInfoRenderer(),  # renders stack_info= kwarg if present
        structlog.processors.UnicodeDecoder(),  # decodes bytes values to str
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S %Z", utc=True),
    ]

    # -- Build mode-specific renderer --------------------------------------------------

    if output in ("tty", "plain"):
        use_colors = output == "tty"

        console_chain: list[structlog.types.Processor] = [_flatten_and_normalize]

        if use_colors:
            level_styles = _build_level_styles()
            console_chain.append(_colorize_event(level_styles))
        else:
            level_styles = None

        console_chain.append(_prepend_logger_name(use_colors=use_colors))

        # Dot-padding after logger name so the prefix length is included.
        console_chain.append(_pad_event(_EVENT_PAD))

        console_chain.append(
            structlog.dev.ConsoleRenderer(
                colors=use_colors,
                sort_keys=False,
                level_styles=level_styles,
                pad_event_to=0,
                exception_formatter=_rich_traceback(
                    use_colors=use_colors,
                    width=None,
                    show_locals=False,
                    word_wrap=True,
                ),
            ),
        )
        processors = shared_processors + console_chain
        logger_factory = structlog.PrintLoggerFactory(file=sys.stderr)

    elif output == "json":
        processors = shared_processors + [
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(serializer=orjson.dumps),
        ]
        logger_factory = structlog.BytesLoggerFactory()

    else:
        # We should never reach this branch thanks to ``OutputMode``
        msg = f"Unknown output mode: {output!r}"
        raise ValueError(msg)

    # -- Apply global structlog configuration ------------------------------------------

    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(min_level=level),
        processors=processors,
        logger_factory=logger_factory,
        cache_logger_on_first_use=True,
    )


# Configure structlog at module import time.
_setup_logger()


# --------------------------------------------------------------------------------------
# Public API
# --------------------------------------------------------------------------------------


def get_logger(name: str | None = None) -> BoundLogger:
    """Return a named structlog logger.

    Parameters
    ----------
    name
        Logger name displayed as ``[name]`` in console output.
        ``None`` returns an unnamed default logger.

    Returns
    -------
    BoundLogger
        A structlog bound logger instance.
    """
    if name is None:
        return structlog.get_logger()

    return structlog.get_logger(logger_name=name)


# --------------------------------------------------------------------------------------
# Manual smoke test
# --------------------------------------------------------------------------------------


if __name__ == "__main__":
    from pydantic import BaseModel

    class Point(BaseModel):
        """Dummy model used for logging demonstration."""

        x: float
        y: float

    obj = Point(x=1, y=2)
    obj_dump = obj.model_dump()
    logger = get_logger("demo")

    logger.debug("debug text", extra_data={"k": 3}, point=obj_dump)
    logger.info("info text (pydantic without .model_dump())", point=obj)
    logger.info("info text", extra_data={"k": 3}, point=obj_dump)
    logger.warning("warning text", extra_data={"k": 3}, point=obj_dump)
    logger.error("error text", extra_data={"k": 3}, point=obj_dump)
    logger.critical("critical text", extra_data={"k": 3}, point=obj_dump)
    logger.exception(
        "exception text (called outside exception)", extra_data={"k": 3}, point=obj_dump
    )
    try:
        _ = 1 / 0
    except ZeroDivisionError:
        logger.exception(
            "exception text (called within exception)", extra_data={"k": 3}, point=obj_dump
        )
