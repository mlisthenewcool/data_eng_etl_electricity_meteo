"""7z archive extraction with progress bar and integrity checks."""

import shutil
import sys
import tempfile
from collections.abc import Callable
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
from data_eng_etl_electricity_meteo.utils.file_hash import FileHasher
from data_eng_etl_electricity_meteo.utils.progress import TqdmExtractCallback

logger = get_logger("extraction")


# --------------------------------------------------------------------------------------
# Types
# --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class ExtractedFileInfo:
    """Extracted file metadata (path, hash, size) returned by ``extract_7z``."""

    path: Path
    file_hash: str
    size_mib: float


# --------------------------------------------------------------------------------------
# Validate SQLite header (GeoPackage)
# --------------------------------------------------------------------------------------


def _validate_sqlite_header(path: Path) -> None:
    """Check the first 16 bytes match the SQLite magic header.

    Parameters
    ----------
    path
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


# --------------------------------------------------------------------------------------
# Public API
# --------------------------------------------------------------------------------------


def extract_7z(
    archive_path: Path,
    target_filename: str,
    dest_dir: Path,
    validate_sqlite: bool = True,
    progress: Callable[[int], ExtractCallback] | None = None,
) -> ExtractedFileInfo:
    """Extract a specific file from a 7z archive.

    Extracts to a temporary directory then moves the result to *dest_dir*.

    Parameters
    ----------
    archive_path
        Path to the .7z archive.
    target_filename
        Name or suffix of file to extract (handles nested paths).
    dest_dir
        Destination directory (created if needed).
    validate_sqlite
        If ``True``, validate SQLite header after extraction.
    progress
        Factory called with ``total_bytes`` (uncompressed size) that returns an
        :class:`~py7zr.callbacks.ExtractCallback`.
        Pass ``None`` (default) to use the built-in tqdm progress bar.

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

    logger.info("Starting extraction", archive=archive_path.name, target=target_filename)

    with tempfile.TemporaryDirectory(prefix="7z_extract_") as tmp_dir:
        tmp_dir_path = Path(tmp_dir)

        # -- Open archive and locate target file ---------------------------------------

        with py7zr.SevenZipFile(archive_path, mode="r") as archive:
            all_files = archive.getnames()

            # Flexible search: IGN archives have inconsistent internal structures
            # e.g., "CONTOURS-IRIS_3-0/iris.gpkg" when we search for "iris.gpkg"
            try:
                target_internal_path = next(f for f in all_files if f.endswith(target_filename))
            except StopIteration:
                raise FileNotFoundInArchiveError(target_filename, archive_path) from None

            logger.debug("Found target in archive", target_path=target_internal_path)

            # -- Get uncompressed size for progress ------------------------------------

            target_info = next(
                info for info in archive.list() if info.filename == target_internal_path
            )
            uncompressed_size = target_info.uncompressed

            # -- Extract with progress tracking ----------------------------------------

            owned_pbar: tqdm | None = None
            if progress is not None:
                callback: ExtractCallback = progress(uncompressed_size)
            else:
                owned_pbar = tqdm(
                    total=uncompressed_size,
                    unit="B",
                    unit_scale=True,
                    desc=f"Extracting {target_filename}",
                    leave=False,
                    file=sys.stderr,
                )
                callback = TqdmExtractCallback(owned_pbar)

            try:
                archive.extract(
                    path=tmp_dir_path, targets=[target_internal_path], callback=callback
                )
            finally:
                if owned_pbar is not None:
                    owned_pbar.close()

            # -- Move to final destination ---------------------------------------------

            extracted_file = tmp_dir_path / target_internal_path

            # Preserve original filename from archive (not the nested internal path)
            dest_path = dest_dir / target_filename
            dest_path.parent.mkdir(parents=True, exist_ok=True)

            # Move to final destination (not atomic across filesystems,
            # but the source temp dir is cleaned up regardless)
            if dest_path.exists():
                dest_path.unlink()
            shutil.move(src=extracted_file, dst=dest_path)

            # -- Validate (optional SQLite check) and compute metadata -----------------

            if validate_sqlite:
                try:
                    _validate_sqlite_header(dest_path)
                except FileIntegrityError:
                    # Avoid leaving a corrupt file that a subsequent run might accept
                    if dest_path.exists():
                        dest_path.unlink()
                    raise

            file_hash = FileHasher.hash_file(dest_path)
            size_mib = round(dest_path.stat().st_size / 1024**2, 2)

            logger.info("Extraction completed", target=target_filename, size_mib=size_mib)

            return ExtractedFileInfo(path=dest_path, size_mib=size_mib, file_hash=file_hash)
