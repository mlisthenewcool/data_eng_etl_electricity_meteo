"""Custom exceptions with automatic attribute extraction for structured logging."""

from pathlib import Path
from typing import Protocol

type _LogValue = str | int | float | bool | list | dict | tuple


class _LogMethod(Protocol):
    """Signature of a structlog bound logger method (e.g. ``logger.error``)."""

    def __call__(self, event: str, /, **kw: _LogValue) -> None: ...


class BaseProjectException(Exception):
    """Base exception with automatic attribute extraction for structured logging via to_dict()."""

    def to_dict(self) -> dict[str, _LogValue]:
        """Extract public attributes as dict for logging."""
        result: dict[str, _LogValue] = {}

        for key, value in self.__dict__.items():
            # Skip private attributes
            if key.startswith("_"):
                continue

            # Skip None values (avoid polluting structured logs)
            if value is None:
                continue
            # Convert Path to string for better logging
            elif isinstance(value, Path):
                result[key] = str(value)
            # Skip complex non-serializable objects (basic safety check)
            elif isinstance(value, (str, int, float, bool)):
                result[key] = value
            # Assume collections contain simple types (or override to_dict if not)
            elif isinstance(value, (list, dict, tuple)):
                result[key] = value
            # Other types (Pydantic models, enums, etc.): fall back to repr()
            else:
                result[key] = repr(value)

        return result

    def log(self, log_method: _LogMethod) -> None:
        """Log this exception with its structured attributes.

        Parameters
        ----------
        log_method : _LogMethod
            A bound logger method (e.g. ``logger.error``, ``logger.critical``).
        """
        log_method(str(self), **self.to_dict())


# ---------------------------------------------------------------------------
# Download/archive errors
# ---------------------------------------------------------------------------
class DownloadError(BaseProjectException):
    """Base exception for download-related failures."""

    # TODO: utile ?


class ExtractionError(BaseProjectException):
    """Base exception for archive extraction failures."""


class ArchiveNotFoundError(ExtractionError):
    """Raised when the archive file does not exist."""

    def __init__(self, path: Path) -> None:
        super().__init__(f"Archive not found: {path}")


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
        self.available_datasets = available_datasets
        super().__init__(f"Dataset {name} does not exist in data catalog.")
