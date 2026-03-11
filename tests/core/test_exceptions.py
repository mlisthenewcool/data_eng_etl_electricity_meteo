"""Unit tests for the core exceptions module."""

from pathlib import Path
from unittest.mock import Mock

from data_eng_etl_electricity_meteo.core.exceptions import (
    ArchiveNotFoundError,
    BronzeStageError,
    ExtractStageError,
    PostgresCredentialsError,
    PostgresLoadError,
)

# --------------------------------------------------------------------------------------
# BaseProjectException.to_dict
# --------------------------------------------------------------------------------------


class TestBaseProjectExceptionToDict:
    def test_returns_public_attributes(self) -> None:
        exc = ArchiveNotFoundError(path=Path("/data/archive.7z"))
        assert exc.to_dict() == {"path": Path("/data/archive.7z")}


# --------------------------------------------------------------------------------------
# BaseProjectException.log
# --------------------------------------------------------------------------------------


class TestBaseProjectExceptionLog:
    def test_calls_log_method_with_structured_attributes(self) -> None:
        exc = ArchiveNotFoundError(path=Path("/data/archive.7z"))
        log_method = Mock()
        exc.log(log_method)
        log_method.assert_called_once_with(str(exc), **exc.to_dict())


# --------------------------------------------------------------------------------------
# PipelineStageError.log
# --------------------------------------------------------------------------------------


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
        cause_data = {f"cause_{k}": v for k, v in cause.to_dict().items()}
        log_method.assert_called_once_with(
            str(exc),
            **exc.to_dict() | cause_data,
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


# --------------------------------------------------------------------------------------
# PipelineStageError.to_dict
# --------------------------------------------------------------------------------------


class TestPipelineStageErrorToDict:
    def test_without_context_returns_empty_dict(self) -> None:
        exc = ExtractStageError()
        assert exc.to_dict() == {}

    def test_with_context_returns_only_kwargs(self) -> None:
        exc = PostgresLoadError("SQL path escapes postgres directory", path="/etc/passwd")
        result = exc.to_dict()
        assert result == {"path": "/etc/passwd"}
        assert "stage" not in result
        assert "context" not in result

    def test_with_multiple_context_keys(self) -> None:
        exc = BronzeStageError("transform failed", table="silver.eco2mix", version="2026-01-01")
        assert exc.to_dict() == {"table": "silver.eco2mix", "version": "2026-01-01"}


# --------------------------------------------------------------------------------------
# PipelineStageError message
# --------------------------------------------------------------------------------------


class TestPipelineStageErrorMessage:
    def test_default_message(self) -> None:
        exc = ExtractStageError()
        assert "extract" in str(exc).lower()

    def test_custom_message(self) -> None:
        exc = ExtractStageError("inner_file required for archive dataset")
        assert str(exc) == "inner_file required for archive dataset"


# --------------------------------------------------------------------------------------
# PostgresCredentialsError.to_dict
# --------------------------------------------------------------------------------------


class TestPostgresCredentialsErrorToDict:
    def test_context_contains_missing_field_and_suggestion(self) -> None:
        exc = PostgresCredentialsError(
            missing_field="postgres_password",
            suggestion="Create secrets/postgres_root_password file.",
        )
        result = exc.to_dict()
        assert result == {
            "missing_field": "postgres_password",
            "suggestion": "Create secrets/postgres_root_password file.",
        }
        assert "stage" not in result


# --------------------------------------------------------------------------------------
# PipelineStageError.log with context
# --------------------------------------------------------------------------------------


class TestPipelineStageErrorLogWithContext:
    def test_log_with_context_only(self) -> None:
        exc = PostgresLoadError("SQL path escapes directory", path="/etc/passwd")
        log_method = Mock()
        exc.log(log_method)
        log_method.assert_called_once_with(
            "SQL path escapes directory",
            path="/etc/passwd",
        )

    def test_log_with_context_and_cause(self) -> None:
        cause = ArchiveNotFoundError(path=Path("/data/archive.7z"))
        exc = ExtractStageError("inner_file required")
        exc.__cause__ = cause
        log_method = Mock()
        exc.log(log_method)
        log_method.assert_called_once_with(
            "inner_file required",
            cause_path=Path("/data/archive.7z"),
            cause_type="ArchiveNotFoundError",
        )
