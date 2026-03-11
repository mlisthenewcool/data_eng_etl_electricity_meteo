"""Custom exceptions with attribute extraction for structured logging."""

from pathlib import Path
from typing import Any, Protocol

from data_eng_etl_electricity_meteo.core.enums import PipelineStage


class _LogMethod(Protocol):
    """Signature of a structlog bound logger method."""

    def __call__(self, event: str, /, **kw: Any) -> None: ...


class BaseProjectException(Exception):
    """Base exception with attribute extraction for structured logging."""

    def to_dict(self) -> dict[str, Any]:
        """Extract public attributes as a dict.

        Value normalization (None filtering, Path conversion, etc.) is handled by the
        structlog processor chain.

        Returns
        -------
        dict[str, Any]
            Mapping of public instance attribute names to their values.
        """
        return {key: value for key, value in self.__dict__.items() if not key.startswith("_")}

    def log(self, log_method: _LogMethod) -> None:
        """Log this exception with its structured attributes.

        Parameters
        ----------
        log_method
            A structlog bound logger method (e.g. ``logger.error``).
        """
        log_method(str(self), **self.to_dict())


# --------------------------------------------------------------------------------------
# Archive errors
# --------------------------------------------------------------------------------------


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


class FileIntegrityError(ExtractionError):
    """Raised when file validation (hash, size, etc.) fails."""

    def __init__(self, path: Path, reason: str) -> None:
        self.path = path
        self.reason = reason
        super().__init__("File integrity check failed.")


# --------------------------------------------------------------------------------------
# Data catalog errors
# --------------------------------------------------------------------------------------


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


class DatasetTypeError(DataCatalogError):
    """Raised when a dataset exists but has an unexpected type."""

    def __init__(self, name: str, expected: str, actual: str) -> None:
        self.name = name
        self.expected = expected
        self.actual = actual
        super().__init__("Dataset has unexpected type.")


# --------------------------------------------------------------------------------------
# Schema validation errors
# --------------------------------------------------------------------------------------


class SchemaValidationError(BaseProjectException):
    """Schema mismatch (columns, types, or value constraints)."""

    def __init__(self, errors: list[str]) -> None:
        self.errors = errors
        super().__init__("Schema validation failed.")


class SourceSchemaDriftError(BaseProjectException):
    """Source API columns changed (added or removed)."""

    def __init__(
        self,
        dataset_name: str,
        added: list[str],
        removed: list[str],
    ) -> None:
        self.dataset_name = dataset_name
        self.added = added
        self.removed = removed
        super().__init__("Source schema drift detected.")


# --------------------------------------------------------------------------------------
# Transformation errors
# --------------------------------------------------------------------------------------


class TransformNotFoundError(BaseProjectException):
    """Raised when no transformation spec is registered for a dataset."""

    def __init__(self, dataset_name: str) -> None:
        self.dataset_name = dataset_name
        super().__init__("Transform spec not found for dataset.")


class TransformValidationError(BaseProjectException):
    """Raised when a silver transform validation fails."""

    def __init__(self, dataset_name: str, reason: str) -> None:
        self.dataset_name = dataset_name
        self.reason = reason
        super().__init__("Transform validation failed.")


# --------------------------------------------------------------------------------------
# Pipeline stage errors
# --------------------------------------------------------------------------------------


class PipelineStageError(BaseProjectException):
    """Raised when a pipeline stage fails."""

    def __init__(
        self,
        stage: PipelineStage,
        message: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        self.stage = stage
        self.context = context or {}
        super().__init__(message or f"Pipeline stage '{stage}' failed.")

    def to_dict(self) -> dict[str, Any]:
        """Return only the structured context (stage is already in the message).

        Returns
        -------
        dict[str, Any]
            Mapping of context keys to their values.
        """
        return dict(self.context)

    def log(self, log_method: _LogMethod) -> None:
        """Log this exception and its cause with structured attributes.

        If the cause is a ``BaseProjectException``, its attributes are merged into the
        log event. Otherwise, the cause type and message are logged as ``cause_type`` /
        ``cause`` fields.

        Parameters
        ----------
        log_method
            A structlog bound logger method (e.g. ``logger.critical``).
        """
        cause = self.__cause__
        if cause is None:
            log_method(str(self), **self.to_dict())
        elif isinstance(cause, BaseProjectException):
            cause_data = {f"cause_{k}": v for k, v in cause.to_dict().items()}
            log_method(
                str(self),
                **self.to_dict() | cause_data,
                cause_type=type(cause).__qualname__,
            )
        else:
            log_method(
                str(self),
                **self.to_dict(),
                cause_type=type(cause).__qualname__,
                cause=str(cause),
            )


class DownloadStageError(PipelineStageError):
    """Raised when the download stage fails."""

    def __init__(self, message: str | None = None, **context: Any) -> None:
        super().__init__(PipelineStage.DOWNLOAD, message, context or None)


class ExtractStageError(PipelineStageError):
    """Raised when archive extraction fails."""

    def __init__(self, message: str | None = None, **context: Any) -> None:
        super().__init__(PipelineStage.EXTRACT, message, context or None)


class BronzeStageError(PipelineStageError):
    """Raised when the bronze transformation stage fails."""

    def __init__(self, message: str | None = None, **context: Any) -> None:
        super().__init__(PipelineStage.BRONZE, message, context or None)


class SilverStageError(PipelineStageError):
    """Raised when the silver transformation stage fails."""

    def __init__(self, message: str | None = None, **context: Any) -> None:
        super().__init__(PipelineStage.SILVER, message, context or None)


class PostgresLoadError(PipelineStageError):
    """Raised when loading silver Parquet into Postgres fails."""

    def __init__(self, message: str | None = None, **context: Any) -> None:
        super().__init__(PipelineStage.LOAD_POSTGRES, message, context or None)


class PostgresCredentialsError(PostgresLoadError):
    """Raised when Postgres credentials are missing from Docker secrets files."""

    def __init__(self, missing_field: str, suggestion: str) -> None:
        super().__init__(
            message="Postgres credentials not configured.",
            missing_field=missing_field,
            suggestion=suggestion,
        )


class GoldStageError(PipelineStageError):
    """Raised when the gold aggregation stage fails."""

    def __init__(self, message: str | None = None, **context: Any) -> None:
        super().__init__(PipelineStage.GOLD, message, context or None)


# --------------------------------------------------------------------------------------
# Visual smoke test
# --------------------------------------------------------------------------------------


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

    # -- DownloadStageError — httpx causes ---------------------------------------------

    _section("DownloadStageError  <-  httpx.ConnectError")
    try:
        try:
            raise httpx.ConnectError(
                "[Errno -2] Name or service not known",
                request=httpx.Request("HEAD", "https://data.example.fr/big_archive.7z"),
            )
        except httpx.HTTPError as err:
            raise DownloadStageError() from err
    except DownloadStageError as error:
        error.log(_logger.critical)
    _end()

    _section("DownloadStageError  <-  httpx.TimeoutException")
    try:
        try:
            raise httpx.ReadTimeout(
                "timed out",
                request=httpx.Request("GET", "https://data.example.fr/big_archive.7z"),
            )
        except httpx.HTTPError as err:
            raise DownloadStageError() from err
    except DownloadStageError as error:
        error.log(_logger.critical)
    _end()

    _section("DownloadStageError  <-  httpx.HTTPStatusError")
    try:
        try:
            _request = httpx.Request("GET", "https://data.example.fr/big_archive.7z")
            _response = httpx.Response(status_code=503, request=_request)
            raise httpx.HTTPStatusError("Server Error", request=_request, response=_response)
        except httpx.HTTPError as err:
            raise DownloadStageError() from err
    except DownloadStageError as error:
        error.log(_logger.critical)
    _end()

    # -- ExtractStageError — archive causes --------------------------------------------

    _section("ExtractStageError  <-  ArchiveNotFoundError")
    try:
        try:
            raise ArchiveNotFoundError(path=Path("/data/landing/archive.7z"))
        except ExtractionError as err:
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
        except ExtractionError as err:
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
        except ExtractionError as err:
            raise ExtractStageError() from err
    except ExtractStageError as error:
        error.log(_logger.critical)
    _end()

    # -- BronzeStageError — transform / duckdb / IO causes -----------------------------

    _section("BronzeStageError  <-  TransformNotFoundError")
    try:
        try:
            raise TransformNotFoundError(dataset_name=_DATASET)
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

    # -- SilverStageError — transform / duckdb / IO causes -----------------------------

    _section("SilverStageError  <-  TransformNotFoundError")
    try:
        try:
            raise TransformNotFoundError(dataset_name=_DATASET)
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
    _end()

    # -- New pattern — message + structured context ------------------------------------

    _section("PostgresLoadError  with message + context (no cause)")
    try:
        raise PostgresLoadError("SQL path escapes postgres directory", path="/etc/passwd")
    except PostgresLoadError as error:
        error.log(_logger.critical)
    _end()

    _section("ExtractStageError  with message + context (no cause)")
    try:
        raise ExtractStageError("inner_file required for archive dataset")
    except ExtractStageError as error:
        error.log(_logger.critical)
    _end()

    _section("PostgresCredentialsError  with context via to_dict")
    try:
        raise PostgresCredentialsError(
            missing_field="postgres_password",
            suggestion="Create secrets/postgres_root_password file.",
        )
    except PostgresCredentialsError as error:
        error.log(_logger.critical)
