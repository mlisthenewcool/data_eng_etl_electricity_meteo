"""Custom exceptions with automatic attribute extraction for structured logging."""

from pathlib import Path
from typing import Any


class BaseProjectException(Exception):
    """Base exception with automatic attribute extraction for structured logging via to_dict()."""

    def to_dict(self) -> dict[str, Any]:
        """Extract public attributes as dict for logger extra={}."""
        result: dict[str, Any] = {}

        for key, value in self.__dict__.items():
            # Skip private attributes
            if key.startswith("_"):
                continue

            # Convert Path to string for better logging
            if isinstance(value, Path):
                result[key] = str(value)
            # Skip complex non-serializable objects (basic safety check)
            elif isinstance(value, (str, int, float, bool, type(None))):
                result[key] = value
            elif isinstance(value, (list, dict, tuple)):
                # Assume collections contain simple types (or override to_dict if not)
                result[key] = value
            else:
                # For other types, use class name as placeholder
                result[key] = f"<{type(value).__name__}>"

        return result


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
        super().__init__(f"File {target_filename} not found in archive: {archive_path.name}")


class FileIntegrityError(BaseProjectException):
    """Raised when file validation (hash, size, etc.) fails."""

    def __init__(self, path: Path, reason: str) -> None:
        super().__init__(f"File integrity check failed for {path.name}: {reason}")


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
        self.validation_errors = validation_errors
        super().__init__(f"Data catalog could not be validated for {path}: {reason}")


class DatasetNotFoundError(DataCatalogError):
    """Raised when a requested dataset is missing from the catalog."""

    def __init__(self, name: str, available_datasets: list[str]) -> None:
        self.available_datasets = available_datasets
        super().__init__(f"Dataset {name} does not exist in data catalog")
