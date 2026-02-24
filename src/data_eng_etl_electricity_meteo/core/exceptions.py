"""Custom exceptions with attribute extraction for structured logging."""

from pathlib import Path
from typing import Any, Protocol

from data_eng_etl_electricity_meteo.core.layers import MedallionLayer, PipelineStage


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


class TransformValidationError(BaseProjectException):
    """Raised when a silver transform validation fails."""

    def __init__(self, dataset_name: str, reason: str) -> None:
        self.dataset_name = dataset_name
        self.reason = reason
        super().__init__("Transform validation failed.")


# ---------------------------------------------------------------------------
# Pipeline stage errors
# ---------------------------------------------------------------------------
class PipelineStageError(BaseProjectException):
    """Raised when a pipeline stage fails."""

    def __init__(self, stage: PipelineStage) -> None:
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

    def __init__(self) -> None:
        super().__init__(PipelineStage.INGEST)


class ExtractStageError(PipelineStageError):
    """Raised when archive extraction fails."""

    def __init__(self) -> None:
        super().__init__(PipelineStage.EXTRACT)


class BronzeStageError(PipelineStageError):
    """Raised when the bronze transformation stage fails."""

    def __init__(self) -> None:
        super().__init__(PipelineStage.BRONZE)


class SilverStageError(PipelineStageError):
    """Raised when the silver transformation stage fails."""

    def __init__(self) -> None:
        super().__init__(PipelineStage.SILVER)


class PostgresLoadError(PipelineStageError):
    """Raised when loading silver Parquet into PostgreSQL fails."""

    def __init__(self) -> None:
        super().__init__(PipelineStage.LOAD_POSTGRES)


# ---------------------------------------------------------------------------
# Visual smoke test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys

    import duckdb
    import httpx

    from data_eng_etl_electricity_meteo.core.logger import get_logger

    _logger = get_logger("exception_demo")
    _DATASET = "fake_dataset"

    def _section(title: str) -> None:
        _logger.info(title)

    def _end() -> None:
        print("", file=sys.stderr)

    # ============================================================
    # 1) IngestStageError — httpx causes
    # ============================================================
    _section("IngestStageError  <-  httpx.ConnectError")
    try:
        try:
            raise httpx.ConnectError(
                "[Errno -2] Name or service not known",
                request=httpx.Request("HEAD", "https://data.example.fr/big_archive.7z"),
            )
        except httpx.HTTPError as err:
            raise IngestStageError() from err
    except IngestStageError as error:
        error.log(_logger.critical)
    _end()

    _section("IngestStageError  <-  httpx.TimeoutException")
    try:
        try:
            raise httpx.ReadTimeout(
                "timed out",
                request=httpx.Request("GET", "https://data.example.fr/big_archive.7z"),
            )
        except httpx.HTTPError as err:
            raise IngestStageError() from err
    except IngestStageError as error:
        error.log(_logger.critical)
    _end()

    _section("IngestStageError  <-  httpx.HTTPStatusError")
    try:
        try:
            _request = httpx.Request("GET", "https://data.example.fr/big_archive.7z")
            _response = httpx.Response(status_code=503, request=_request)
            raise httpx.HTTPStatusError("Server Error", request=_request, response=_response)
        except httpx.HTTPError as err:
            raise IngestStageError() from err
    except IngestStageError as error:
        error.log(_logger.critical)
    _end()

    # ============================================================
    # 2) ExtractStageError — archive causes
    # ============================================================
    _section("ExtractStageError  <-  ArchiveNotFoundError")
    try:
        try:
            raise ArchiveNotFoundError(path=Path("/data/landing/archive.7z"))
        except (ArchiveNotFoundError, FileNotFoundInArchiveError, FileIntegrityError) as err:
            raise ExtractStageError() from err
    except ExtractStageError as error:
        error.log(_logger.critical)
    _end()

    _section("ExtractStageError  <-  FileNotFoundInArchiveError")
    try:
        try:
            raise FileNotFoundInArchiveError(
                target_filename="data.gpkg",
                archive_path=Path("/data/landing/archive.7z"),
            )
        except (ArchiveNotFoundError, FileNotFoundInArchiveError, FileIntegrityError) as err:
            raise ExtractStageError() from err
    except ExtractStageError as error:
        error.log(_logger.critical)
    _end()

    _section("ExtractStageError  <-  FileIntegrityError")
    try:
        try:
            raise FileIntegrityError(
                path=Path("/data/landing/data.gpkg"),
                reason="SHA-256 mismatch: expected abc123, got def456",
            )
        except (ArchiveNotFoundError, FileNotFoundInArchiveError, FileIntegrityError) as err:
            raise ExtractStageError() from err
    except ExtractStageError as error:
        error.log(_logger.critical)
    _end()

    # ============================================================
    # 3) BronzeStageError — transform / duckdb / IO causes
    # ============================================================
    _section("BronzeStageError  <-  TransformNotFoundError")
    try:
        try:
            raise TransformNotFoundError(dataset_name=_DATASET, layer=MedallionLayer.BRONZE)
        except (TransformNotFoundError, duckdb.Error, OSError) as err:
            raise BronzeStageError() from err
    except BronzeStageError as error:
        error.log(_logger.critical)
    _end()

    _section("BronzeStageError  <-  duckdb.IOException")
    try:
        try:
            raise duckdb.IOException(
                "Could not open file '/data/bronze/v1.parquet': Permission denied"
            )
        except (TransformNotFoundError, duckdb.Error, OSError) as err:
            raise BronzeStageError() from err
    except BronzeStageError as error:
        error.log(_logger.critical)
    _end()

    _section("BronzeStageError  <-  OSError")
    try:
        try:
            raise OSError("[Errno 28] No space left on device: '/data/bronze/v1.parquet'")
        except (TransformNotFoundError, duckdb.Error, OSError) as err:
            raise BronzeStageError() from err
    except BronzeStageError as error:
        error.log(_logger.critical)
    _end()

    # ============================================================
    # 4) SilverStageError — transform / duckdb / IO causes
    # ============================================================
    _section("SilverStageError  <-  TransformNotFoundError")
    try:
        try:
            raise TransformNotFoundError(dataset_name=_DATASET, layer=MedallionLayer.SILVER)
        except (TransformNotFoundError, duckdb.Error, OSError) as err:
            raise SilverStageError() from err
    except SilverStageError as error:
        error.log(_logger.critical)
    _end()

    _section("SilverStageError  <-  duckdb.ConversionException")
    try:
        try:
            raise duckdb.ConversionException("Could not cast VARCHAR to INTEGER")
        except (TransformNotFoundError, duckdb.Error, OSError) as err:
            raise SilverStageError() from err
    except SilverStageError as error:
        error.log(_logger.critical)
    _end()

    _section("SilverStageError  <-  OSError")
    try:
        try:
            raise PermissionError("[Errno 13] Permission denied: '/data/silver/current.parquet'")
        except (TransformNotFoundError, duckdb.Error, OSError) as err:
            raise SilverStageError() from err
    except SilverStageError as error:
        error.log(_logger.critical)
