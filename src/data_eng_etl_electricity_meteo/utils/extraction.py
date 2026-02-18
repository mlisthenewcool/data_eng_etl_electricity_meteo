"""7z archive extraction with progress bar and integrity checks."""

import shutil
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

import py7zr
from py7zr.callbacks import ExtractCallback
from tqdm import tqdm

from data_eng_etl_electricity_meteo.core.exceptions import (
    ArchiveNotFoundError,
    FileIntegrityError,
    FileNotFoundInArchiveError,
)
from data_eng_etl_electricity_meteo.core.logger import get_logger
from data_eng_etl_electricity_meteo.core.settings import settings
from data_eng_etl_electricity_meteo.utils.file_hash import FileHasher

logger = get_logger("extraction")


@dataclass(frozen=True)
class ExtractionInfo:
    """Extraction information with archive context (for pipeline traceability)."""

    archive_path: Path
    file_path: Path
    file_hash: str
    size_mib: float

    # def to_dict(self) -> dict[str, str | float]:
    #     """Serialize to a JSON-compatible dict (for Airflow XCom)."""
    #     return {
    #         "archive_path": str(self.archive_path),
    #         "file_path": str(self.file_path),
    #         "file_hash": self.file_hash,
    #         "size_mib": self.size_mib,
    #     }


@dataclass(frozen=True)
class ExtractedFileInfo:
    """Extracted file metadata only (no archive context)."""

    path: Path
    file_hash: str
    size_mib: float


class _TqdmExtractCallback(ExtractCallback):
    """Bridge between py7zr extraction and tqdm progress bar."""

    def __init__(self, pbar: tqdm):
        self.pbar = pbar

    def report_start(self, processing_file_path: str, processing_bytes: str) -> None:
        """Signal that extraction of a file begins."""

    def report_end(self, processing_file_path: str, wrote_bytes: str) -> None:
        """Signal that extraction of a file ends."""

    def report_update(self, decompressed_bytes: str) -> None:
        """Update the progress bar with decompressed bytes."""
        self.pbar.update(int(decompressed_bytes))

    def report_start_preparation(self) -> None:
        """Signal that archive preparation starts."""

    def report_warning(self, message: str) -> None:
        """Handle a py7zr warning."""

    def report_postprocess(self) -> None:
        """Signal that post-processing starts."""


def _validate_sqlite_header(path: Path) -> None:
    """Check the first 16 bytes match the SQLite magic header.

    Parameters
    ----------
    path:
        File to validate (GeoPackage files are SQLite databases).

    Raises
    ------
    FileIntegrityError
        If file is missing, empty, or has an invalid header.
    """
    if not path.exists():
        raise FileIntegrityError(path, reason="File does not exist")

    if path.stat().st_size == 0:
        raise FileIntegrityError(path, reason="File is empty")

    try:
        with path.open(mode="rb") as f:
            header = f.read(16)
            if header != b"SQLite format 3\x00":
                raise FileIntegrityError(path, reason="Invalid SQLite/GeoPackage header")
    except OSError as error:
        raise FileIntegrityError(path, reason=f"Could not read file header: {error}") from error


def extract_7z(
    archive_path: Path,
    target_filename: str,
    dest_dir: Path,
    validate_sqlite: bool = True,
) -> ExtractedFileInfo:
    """Extract a specific file from a 7z archive.

    Extracts to a temporary directory then moves atomically to *dest_dir*.

    Parameters
    ----------
    archive_path:
        Path to the .7z archive.
    target_filename:
        Name or suffix of file to extract (handles nested paths).
    dest_dir:
        Destination directory (created if needed).
    validate_sqlite:
        If ``True``, validate SQLite header after extraction.

    Returns
    -------
    ExtractedFileInfo
        Extracted file path, hash, and size.

    Raises
    ------
    ArchiveNotFoundError
        If *archive_path* doesn't exist.
    FileNotFoundInArchiveError
        If *target_filename* not found in archive.
    FileIntegrityError
        If validation enabled and file is invalid.
    """
    if not archive_path.exists():
        raise ArchiveNotFoundError(archive_path)

    logger.debug("Starting extraction", archive=archive_path.name, target=target_filename)

    with tempfile.TemporaryDirectory(prefix="7z_extract_") as tmp_dir:
        tmp_dir_path = Path(tmp_dir)

        with py7zr.SevenZipFile(archive_path, mode="r") as archive:
            all_files = archive.getnames()

            # Flexible search: IGN archives have inconsistent internal structures
            # e.g., "CONTOURS-IRIS_3-0/iris.gpkg" when we search for "iris.gpkg"
            try:
                target_internal_path = next(f for f in all_files if f.endswith(target_filename))
            except StopIteration:
                raise FileNotFoundInArchiveError(target_filename, archive_path) from None

            logger.debug("Found target in archive", target_path=target_internal_path)

            # Get uncompressed size for progress bar
            target_info = next(
                info for info in archive.list() if info.filename == target_internal_path
            )
            uncompressed_size = target_info.uncompressed

            # Extract to temp directory with progress
            with tqdm(
                total=uncompressed_size,
                unit="B",
                unit_scale=True,
                desc=f"Extracting {target_filename}",
                leave=False,
                file=sys.stderr,  # TqdmToLoguru(logger.info) if settings.is_running_on_airflow else
                mininterval=5.0 if settings.is_running_on_airflow else 1.0,
            ) as pbar:
                callback = _TqdmExtractCallback(pbar)

                archive.extract(
                    path=tmp_dir_path, targets=[target_internal_path], callback=callback
                )

            extracted_file = tmp_dir_path / target_internal_path

            # Compute final destination path (preserve original filename from archive)
            dest_path = dest_dir / target_filename
            dest_path.parent.mkdir(parents=True, exist_ok=True)

            # Atomic move to final destination
            if dest_path.exists():
                dest_path.unlink()
            shutil.move(src=extracted_file, dst=dest_path)

            # Optional SQLite validation for GeoPackage files
            if validate_sqlite:
                try:
                    _validate_sqlite_header(dest_path)
                except FileIntegrityError:
                    # Clean up invalid file
                    if dest_path.exists():
                        dest_path.unlink()
                    raise

            file_hash = FileHasher.hash_file(dest_path)
            size_mib = round(dest_path.stat().st_size / 1024**2, 2)

            logger.debug(
                "Extraction completed", path=dest_path, size_mib=size_mib, file_hash=file_hash
            )

            return ExtractedFileInfo(path=dest_path, size_mib=size_mib, file_hash=file_hash)
