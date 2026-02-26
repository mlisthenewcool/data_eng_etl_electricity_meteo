"""Unit tests for the core exceptions module."""

from pathlib import Path
from unittest.mock import Mock

from data_eng_etl_electricity_meteo.core.exceptions import (
    ArchiveNotFoundError,
    ExtractStageError,
)

# ---------------------------------------------------------------------------
# BaseProjectException.to_dict
# ---------------------------------------------------------------------------


class TestBaseProjectExceptionToDict:
    def test_returns_public_attributes(self) -> None:
        exc = ArchiveNotFoundError(path=Path("/data/archive.7z"))
        assert exc.to_dict() == {"path": Path("/data/archive.7z")}


# ---------------------------------------------------------------------------
# BaseProjectException.log
# ---------------------------------------------------------------------------


class TestBaseProjectExceptionLog:
    def test_calls_log_method_with_structured_attributes(self) -> None:
        exc = ArchiveNotFoundError(path=Path("/data/archive.7z"))
        log_method = Mock()
        exc.log(log_method)
        log_method.assert_called_once_with(str(exc), **exc.to_dict())


# ---------------------------------------------------------------------------
# PipelineStageError.log
# ---------------------------------------------------------------------------


class TestPipelineStageErrorLog:
    def test_without_cause(self) -> None:
        exc = ExtractStageError()
        log_method = Mock()
        exc.log(log_method)
        log_method.assert_called_once_with(str(exc), **exc.to_dict())

    def test_with_project_exception_cause_merges_attributes(self) -> None:
        cause = ArchiveNotFoundError(path=Path("/data/archive.7z"))
        exc = ExtractStageError()
        exc.__cause__ = cause
        log_method = Mock()
        exc.log(log_method)
        log_method.assert_called_once_with(
            str(exc),
            **exc.to_dict() | cause.to_dict(),
            cause_type=type(cause).__qualname__,
        )

    def test_with_generic_exception_cause(self) -> None:
        cause = OSError("No space left on device")
        exc = ExtractStageError()
        exc.__cause__ = cause
        log_method = Mock()
        exc.log(log_method)
        log_method.assert_called_once_with(
            str(exc),
            **exc.to_dict(),
            cause_type=type(cause).__qualname__,
            cause=str(cause),
        )
