"""Custom exceptions with attribute extraction for structured logging."""

from pathlib import Path
from typing import Any, Protocol

from data_eng_etl_electricity_meteo.core.layers import MedallionLayer


class _LogMethod(Protocol):
    """Signature of a structlog bound logger method."""

    def __call__(self, event: str, /, **kw: Any) -> None: ...


class BaseProjectException(Exception):
    """Base exception with attribute extraction for structured logging."""

    def to_dict(self) -> dict[str, Any]:
        """Extract public attributes as a dict.

        Value normalization (None filtering, Path conversion, etc.) is handled by the
        structlog processor chain.
        """
        return {key: value for key, value in self.__dict__.items() if not key.startswith("_")}

    def log(self, log_method: _LogMethod) -> None:
        """Log this exception with its structured attributes."""
        log_method(str(self), **self.to_dict())


# ---------------------------------------------------------------------------
# Archive errors
# ---------------------------------------------------------------------------
class ExtractionError(BaseProjectException):
    """Base exception for archive extraction failures."""


class ArchiveNotFoundError(ExtractionError):
    """Raised when the archive file does not exist."""

    def __init__(self, path: Path) -> None:
        self.path = path
        super().__init__("Archive not found.")


class FileNotFoundInArchiveError(ExtractionError):
    """Raised when the target file is not found within the archive."""

    def __init__(self, target_filename: str, archive_path: Path) -> None:
        self.target_filename = target_filename
        self.archive_path = archive_path
        super().__init__("File not found in archive.")


class FileIntegrityError(BaseProjectException):
    """Raised when file validation (hash, size, etc.) fails."""

    def __init__(self, path: Path, reason: str) -> None:
        self.path = path
        self.reason = reason
        super().__init__("File integrity check failed.")


# ---------------------------------------------------------------------------
# Data catalog errors
# ---------------------------------------------------------------------------
class DataCatalogError(BaseProjectException):
    """Base exception for data catalog related failures."""


class InvalidCatalogError(DataCatalogError):
    """Raised when the data catalog YAML could not be parsed or validated."""

    def __init__(
        self, path: Path, reason: str, validation_errors: dict[str, str] | None = None
    ) -> None:
        self.path = path
        self.reason = reason
        self.validation_errors = validation_errors
        super().__init__("Data catalog could not be validated.")


class DatasetNotFoundError(DataCatalogError):
    """Raised when a requested dataset is missing from the catalog."""

    def __init__(self, name: str, available_datasets: list[str]) -> None:
        self.name = name
        self.available_datasets = available_datasets
        super().__init__("Dataset does not exist in data catalog.")


# ---------------------------------------------------------------------------
# Airflow context errors
# ---------------------------------------------------------------------------
class AirflowContextError(BaseProjectException):
    """Raised when an operation requires a specific Airflow context."""

    def __init__(
        self,
        operation: str,
        expected_context: str,
        suggestion: str,
    ) -> None:
        self.operation = operation
        self.expected_context = expected_context
        super().__init__(f"Invalid Airflow context: {suggestion}.")


# ---------------------------------------------------------------------------
# Transformation errors
# ---------------------------------------------------------------------------
class TransformNotFoundError(BaseProjectException):
    """Raised when no transformation is registered for a dataset/layer pair."""

    def __init__(self, dataset_name: str, layer: MedallionLayer) -> None:
        self.dataset_name = dataset_name
        self.layer = layer
        super().__init__("Transform not found for dataset.")


# ---------------------------------------------------------------------------
# Pipeline stage errors
# ---------------------------------------------------------------------------
class PipelineStageError(BaseProjectException):
    """Raised when a pipeline stage fails."""

    def __init__(self, dataset_name: str, stage: MedallionLayer) -> None:
        self.dataset_name = dataset_name
        self.stage = stage
        super().__init__(f"Pipeline failed at {stage} stage.")

    def log(self, log_method: _LogMethod) -> None:
        """Log this exception and its cause with structured attributes."""
        cause = self.__cause__
        if cause is None:
            log_method(str(self), **self.to_dict())
        elif isinstance(cause, BaseProjectException):
            log_method(
                str(self),
                **self.to_dict() | cause.to_dict(),
                cause_type=type(cause).__qualname__,
            )
        else:
            log_method(
                str(self),
                **self.to_dict(),
                cause_type=type(cause).__qualname__,
                cause=str(cause),
            )


class IngestStageError(PipelineStageError):
    """Raised when the ingest (download) stage fails."""

    def __init__(self, dataset_name: str) -> None:
        super().__init__(dataset_name, "landing")


class ExtractStageError(PipelineStageError):
    """Raised when archive extraction fails."""

    def __init__(self, dataset_name: str) -> None:
        super().__init__(dataset_name, "landing")


class BronzeStageError(PipelineStageError):
    """Raised when the bronze transformation stage fails."""

    def __init__(self, dataset_name: str) -> None:
        super().__init__(dataset_name, "bronze")


class SilverStageError(PipelineStageError):
    """Raised when the silver transformation stage fails."""

    def __init__(self, dataset_name: str) -> None:
        super().__init__(dataset_name, "silver")
