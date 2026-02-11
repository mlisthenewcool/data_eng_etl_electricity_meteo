"""Loguru-based logger with extra={} support and automatic environment detection.

Adapts output to the execution environment:
- Terminal: colored output to stderr with timestamps and structured extra fields
- Airflow: colored output routed to Airflow's task logger
- Notebook (Marimo): same as terminal
- Otherwise: not colored
"""

import logging
import re
import sys
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Callable

from loguru import logger as _loguru_logger

from data_eng_etl_electricity_meteo.core.settings import settings

if TYPE_CHECKING:
    from loguru import Logger, Message, Record


# ---------------------------------------------------------------------------
# Environment detection
# ---------------------------------------------------------------------------
_ON_AIRFLOW = settings.is_running_on_airflow
_ON_NOTEBOOK = settings.is_running_in_notebook


# ---------------------------------------------------------------------------
# Color palettes (Loguru-native markup tags)
# ---------------------------------------------------------------------------
class LoguruMarkup(StrEnum):
    """Loguru color tags adapted to the detected terminal background."""

    # fmt:off
    key_open  = "<magenta><bold>"
    key_close = "</bold></magenta>"
    dim_open  = "<dim>"
    dim_close = "</dim>"
    # fmt:on


# ---------------------------------------------------------------------------
# ANSI / Loguru-tag sanitization helpers
# ---------------------------------------------------------------------------
_ANSI_PATTERN = re.compile(r"\033\[[0-9;]*m")


def _strip_ansi(text: str) -> str:
    return _ANSI_PATTERN.sub(repl="", string=text)


def _escape_loguru_tags(text: str) -> str:
    r"""Escape ``<`` so user content is never parsed as a Loguru color tag.

    Loguru interprets ``\<`` as a literal ``<``, preventing tag parsing.
    """
    return text.replace("<", r"\<")


def _escape_braces(text: str) -> str:
    """Escape ``{`` and ``}`` so user content is safe inside a format template."""
    return text.replace("{", "{{").replace("}", "}}")


def _safe_str(value: Any) -> str:
    """Convert *value* to a display-safe string (no ANSI, no Loguru tags, no braces)."""
    try:
        return _escape_braces(_escape_loguru_tags(_strip_ansi(str(value))))
    except Exception:
        return r"\<REPR_ERROR>"


# ---------------------------------------------------------------------------
# Extra-fields tree formatter (builds Loguru-tag template fragments)
# ---------------------------------------------------------------------------
_INDENT = 22 if _ON_AIRFLOW else 35
_INDENT_STEP = 4


def _compute_prefix(level: int) -> str:
    # ├─▶ ╰─▶
    current_indent = " " * (_INDENT + (level * _INDENT_STEP))
    return f"\n{current_indent}{LoguruMarkup.dim_open}╰─▶ {LoguruMarkup.dim_close}"


def _format_value_recursive(extra: Any, level: int) -> str:
    """Format extras recursively with tree-style indentation for nested dicts.

    Returns a fragment of **Loguru format template** (with native color tags).
    """
    # Terminal case: non-dict values are converted to safe strings
    if not isinstance(extra, dict):
        return _safe_str(extra)

    # Empty dict
    if not extra:
        return ""

    line_prefix = _compute_prefix(level=level)

    parts: list[str] = []
    for key, val in extra.items():
        key_fmt = f"{LoguruMarkup.key_open}{_safe_str(key)}{LoguruMarkup.key_close}"

        if isinstance(val, dict) and val:
            val_fmt = _format_value_recursive(val, level + 1)
            parts.append(f"{key_fmt}{val_fmt}")
        else:
            val_fmt = _safe_str(val)  # no LoguruMarkup to adapt automatically to dark/light themes
            parts.append(f"{key_fmt}={val_fmt}")

    result = line_prefix.join(parts)
    if level > 0:
        return line_prefix + result

    return result


# ---------------------------------------------------------------------------
# Dynamic Loguru format function
# ---------------------------------------------------------------------------
# Plain strings (not f-strings): single braces are Loguru placeholders.
_BASE_TERMINAL = (
    "<dim>{time:YYYY-MM-DD HH:mm:ss zz} | </dim>"
    "<level>{level: <8}</level> <dim>|</dim> "
    "<level>{message}</level> "
    "<dim>| [{file}:{function}:{line}]</dim>"
)
_BASE_AIRFLOW = "<level>{message}</level>"


def _make_format_func(base_template: str) -> "Callable[[Record], str]":
    r"""Return a Loguru-compatible callable format function.

    The returned function inspects the record's *extra* dict at emit-time and
    builds a format template string that includes native Loguru color tags for
    the extras tree.  Loguru parses those tags, respects ``colorize``, and
    handles ``{exception}`` correctly.

    Note:
       With a callable format, Loguru does **not** auto-append
       ``\\n{exception}``; we must include it ourselves.
    """

    def _format(record: "Record") -> str:
        extra = record.get("extra", {})
        if extra:
            prefix = _compute_prefix(level=0)
            formatted = _format_value_recursive(extra, level=0)
            extra_part = prefix + formatted
        else:
            extra_part = ""
        return base_template + extra_part + "\n{exception}"

    return _format


# ---------------------------------------------------------------------------
# Airflow log sink
# ---------------------------------------------------------------------------
def _airflow_sink(message: "Message") -> None:
    """Route log messages to Airflow's task logger with correct severity level.

    In Airflow 3.x, the task subprocess sends structured logs over a dedicated
    JSON socket to the supervisor.  The root logger's ``"to_supervisor"``
    handler (set up by Airflow/structlog) serializes each record **with the
    originating logger name** — this is what populates the per-logger tabs
    (e.g. ``MY_LOGGER``) in the task-log UI.

    Airflow's ``extra_logger_names`` config also attaches a ``"console"``
    handler **directly** to the named logger with ``propagate=True``.  This
    causes every message to appear **twice**: once via the direct ``"console"``
    handler (→ stdout → ``task.stdout`` tab) and once via propagation to the
    root's ``"to_supervisor"`` handler (→ ``MY_LOGGER`` tab).

    To prevent the duplicate we lazily clear any handlers that Airflow's
    ``dictConfig`` may have added.  The clearing is repeated on every call
    because ``dictConfig`` can run **after** our module-level ``_configure``
    (e.g. when the task subprocess is forked).  We also force the level to
    ``DEBUG`` so the stdlib logger never silently drops messages that Loguru
    already filtered.
    """
    py_logger = logging.getLogger(name=settings.logger_name)

    # Guard: Airflow's dictConfig may (re-)attach handlers after module import.
    if py_logger.handlers:
        py_logger.handlers.clear()
    if py_logger.level != logging.DEBUG:
        py_logger.setLevel(logging.DEBUG)

    record = message.record
    level_name = record["level"].name
    level_no = logging.getLevelName(level=level_name)
    py_logger.log(level=level_no, msg=message)


# ---------------------------------------------------------------------------
# Logger singleton
# ---------------------------------------------------------------------------
class LoguruLogger:
    """Singleton Loguru wrapper bridging stdlib extra={} pattern with Loguru's bind()."""

    _instance: "LoguruLogger | None" = None
    _logger: "Logger"

    def __new__(cls, level: str) -> "LoguruLogger":
        """Return singleton instance, creating on first call."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._configure(level)
        return cls._instance

    def _configure(self, level: str) -> None:
        _loguru_logger.remove()

        if _ON_AIRFLOW:
            sink = _airflow_sink
            base = _BASE_AIRFLOW
        else:
            sink = sys.stderr
            base = _BASE_TERMINAL

        format_func = _make_format_func(base_template=base)
        colorize = _ON_AIRFLOW or _ON_NOTEBOOK or sys.stderr.isatty()

        _loguru_logger.add(
            sink=sink,
            level=level,
            format=format_func,
            colorize=colorize,
            backtrace=True,
            diagnose=True,
        )

        self._logger = _loguru_logger

    def _log(
        self, level: str, message: str, extra: dict[str, Any] | None = None, exc_info: bool = False
    ) -> None:
        log_extra = extra or {}

        bound = self._logger.bind(**log_extra)
        # depth=2 is required so Loguru correctly identifies the caller's
        # filename and line number, skipping the _log() and info/debug() wrappers.
        getattr(bound.opt(depth=2, exception=exc_info), level)(message)

    def debug(self, message: str, /, extra: dict[str, Any] | None = None) -> None:
        """Log a debug message."""
        self._log("debug", message, extra)

    def info(self, message: str, /, extra: dict[str, Any] | None = None) -> None:
        """Log an info message."""
        self._log("info", message, extra)

    def warning(self, message: str, /, extra: dict[str, Any] | None = None) -> None:
        """Log a warning message."""
        self._log("warning", message, extra)

    def error(self, message: str, /, extra: dict[str, Any] | None = None) -> None:
        """Log an error message."""
        self._log("error", message, extra)

    def critical(self, message: str, /, extra: dict[str, Any] | None = None) -> None:
        """Log a critical message."""
        self._log("critical", message, extra)

    def exception(self, message: str, /, extra: dict[str, Any] | None = None) -> None:
        """Log error with traceback. Must be called inside an exception handler."""
        if sys.exc_info()[0] is None:
            extra = extra or {}
            extra["warning"] = "You called logger.exception() with no active exception."
            exc_info = False
        else:
            exc_info = True
        self._log("error", message, extra, exc_info)

    @classmethod
    def _reset(cls) -> None:
        cls._instance = None


class TqdmToLoguru:
    """File-like proxy redirecting tqdm progress output to Loguru."""

    def __init__(self, logger_func: Callable):
        self.logger_func = logger_func

    def write(self, buf: str) -> None:
        """Forward cleaned tqdm buffer to the logger."""
        message = buf.strip("\r\n\t ")
        if message:
            self.logger_func(message)


logger = LoguruLogger(level=settings.app_logging_level)


if __name__ == "__main__":
    from pathlib import Path

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

    # ANSI injection test — codes should be stripped
    logger.info("ANSI test", extra={"\x1b[1;31mred as key": "\033[1;31mred as value"})

    # Loguru tag injection test — tags should be escaped
    logger.info("Tag test", extra={"<red>evil": "<bold>bad</bold>"})

    # Nested extras
    logger.info("Nested", extra={"outer": {"inner_key": "inner_val", "deep": {"a": 1}}})

    logger.exception("No active exception")

    try:
        _ = 1 / 0
    except ZeroDivisionError:
        logger.exception("Division error", extra={"context": "test"})

    logger.info("After exception")
