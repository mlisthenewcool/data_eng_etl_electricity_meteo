"""Visual test: raise every low-level exception through ``PipelineStageError.log()``."""

from pathlib import Path

import duckdb
import httpx

from data_eng_etl_electricity_meteo.core.exceptions import (
    ArchiveNotFoundError,
    BronzeStageError,
    ExtractStageError,
    FileIntegrityError,
    FileNotFoundInArchiveError,
    IngestStageError,
    SilverStageError,
    TransformNotFoundError,
)
from data_eng_etl_electricity_meteo.core.logger import logger

DATASET = "fake_dataset"
SEP = "=" * 72


def _section(title: str) -> None:
    logger.info(f"\n{SEP}\n{title}\n{SEP}")


# ============================================================
# 1) IngestStageError — httpx causes
# ============================================================
_section("IngestStageError  ←  httpx.ConnectError")
try:
    try:
        raise httpx.ConnectError(
            "[Errno -2] Name or service not known",
            request=httpx.Request("HEAD", "https://data.example.fr/big_archive.7z"),
        )
    except httpx.HTTPError as err:
        raise IngestStageError(DATASET) from err
except IngestStageError as error:
    error.log(logger.critical)

_section("IngestStageError  ←  httpx.TimeoutException")
try:
    try:
        raise httpx.ReadTimeout(
            "timed out",
            request=httpx.Request("GET", "https://data.example.fr/big_archive.7z"),
        )
    except httpx.HTTPError as err:
        raise IngestStageError(DATASET) from err
except IngestStageError as error:
    error.log(logger.critical)

_section("IngestStageError  ←  httpx.HTTPStatusError")
try:
    try:
        request = httpx.Request("GET", "https://data.example.fr/big_archive.7z")
        response = httpx.Response(status_code=503, request=request)
        raise httpx.HTTPStatusError(
            "Server Error",
            request=request,
            response=response,
        )
    except httpx.HTTPError as err:
        raise IngestStageError(DATASET) from err
except IngestStageError as error:
    error.log(logger.critical)

# ============================================================
# 2) ExtractStageError — archive causes
# ============================================================
_section("ExtractStageError  ←  ArchiveNotFoundError")
try:
    try:
        raise ArchiveNotFoundError(path=Path("/data/landing/archive.7z"))
    except (
        ArchiveNotFoundError,
        FileNotFoundInArchiveError,
        FileIntegrityError,
        ValueError,
    ) as err:
        raise ExtractStageError(DATASET) from err
except ExtractStageError as error:
    error.log(logger.critical)

_section("ExtractStageError  ←  FileNotFoundInArchiveError")
try:
    try:
        raise FileNotFoundInArchiveError(
            target_filename="data.gpkg",
            archive_path=Path("/data/landing/archive.7z"),
        )
    except (
        ArchiveNotFoundError,
        FileNotFoundInArchiveError,
        FileIntegrityError,
        ValueError,
    ) as err:
        raise ExtractStageError(DATASET) from err
except ExtractStageError as error:
    error.log(logger.critical)

_section("ExtractStageError  ←  FileIntegrityError")
try:
    try:
        raise FileIntegrityError(
            path=Path("/data/landing/data.gpkg"),
            reason="SHA-256 mismatch: expected abc123, got def456",
        )
    except (
        ArchiveNotFoundError,
        FileNotFoundInArchiveError,
        FileIntegrityError,
        ValueError,
    ) as err:
        raise ExtractStageError(DATASET) from err
except ExtractStageError as error:
    error.log(logger.critical)

_section("ExtractStageError  ←  ValueError")
try:
    try:
        raise ValueError("Dataset fake_dataset is not an archive or missing inner_file")
    except (
        ArchiveNotFoundError,
        FileNotFoundInArchiveError,
        FileIntegrityError,
        ValueError,
    ) as err:
        raise ExtractStageError(DATASET) from err
except ExtractStageError as error:
    error.log(logger.critical)

# ============================================================
# 3) BronzeStageError — transform / duckdb / IO causes
# ============================================================
_section("BronzeStageError  ←  TransformNotFoundError")
try:
    try:
        raise TransformNotFoundError(dataset_name=DATASET, layer="bronze")
    except (TransformNotFoundError, duckdb.Error, OSError) as err:
        raise BronzeStageError(DATASET) from err
except BronzeStageError as error:
    error.log(logger.critical)

_section("BronzeStageError  ←  duckdb.IOException")
try:
    try:
        raise duckdb.IOException("Could not open file '/data/bronze/v1.parquet': Permission denied")
    except (TransformNotFoundError, duckdb.Error, OSError) as err:
        raise BronzeStageError(DATASET) from err
except BronzeStageError as error:
    error.log(logger.critical)

_section("BronzeStageError  ←  OSError")
try:
    try:
        raise OSError("[Errno 28] No space left on device: '/data/bronze/v1.parquet'")
    except (TransformNotFoundError, duckdb.Error, OSError) as err:
        raise BronzeStageError(DATASET) from err
except BronzeStageError as error:
    error.log(logger.critical)

# ============================================================
# 4) SilverStageError — transform / duckdb / IO causes
# ============================================================
_section("SilverStageError  ←  TransformNotFoundError")
try:
    try:
        raise TransformNotFoundError(dataset_name=DATASET, layer="silver")
    except (TransformNotFoundError, duckdb.Error, OSError) as err:
        raise SilverStageError(DATASET) from err
except SilverStageError as error:
    error.log(logger.critical)

_section("SilverStageError  ←  duckdb.ConversionException")
try:
    try:
        raise duckdb.ConversionException("Could not cast VARCHAR to INTEGER")
    except (TransformNotFoundError, duckdb.Error, OSError) as err:
        raise SilverStageError(DATASET) from err
except SilverStageError as error:
    error.log(logger.critical)

_section("SilverStageError  ←  OSError")
try:
    try:
        raise PermissionError("[Errno 13] Permission denied: '/data/silver/current.parquet'")
    except (TransformNotFoundError, duckdb.Error, OSError) as err:
        raise SilverStageError(DATASET) from err
except SilverStageError as error:
    error.log(logger.critical)
