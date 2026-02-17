"""Typed results for pipeline stages (composition-based).

Each pipeline stage produces its own info type. Stage results compose prior results
rather than inheriting them, keeping each layer isolated while preserving full
traceability across the pipeline.

Architecture
------------

- Pipeline functions (typed) ←→ Airflow tasks (dicts via XCom)

    - ingest()        → DownloadStageResult   → model_dump() → XCom dict
    - extract()       → DownloadStageResult (with extraction_info)
    - to_bronze()     → BronzeStageResult
    - to_silver()     → SilverStageResult
    - (to_gold)       → GoldStageResult
"""

from pathlib import Path

from data_eng_etl_electricity_meteo.core.pydantic_base import StrictModel
from data_eng_etl_electricity_meteo.utils.download import HttpDownloadInfo
from data_eng_etl_electricity_meteo.utils.extraction import ExtractionInfo
from data_eng_etl_electricity_meteo.utils.remote_metadata import RemoteFileMetadata

__all__: list[str] = [
    "IngestionDecision",
    "DownloadStageResult",
    "BronzeInfo",
    "BronzeStageResult",
    "SilverInfo",
    "SilverStageResult",
    "GoldStageResult",
    "PipelineRunSnapshot",
]


# =============================================================================
# Stage-specific info types
# =============================================================================


class BronzeInfo(StrictModel):
    """Metrics produced by the bronze conversion stage."""

    file_size_mib: float
    row_count: int
    columns: list[str]


class SilverInfo(StrictModel):
    """Metrics produced by the silver transformation stage."""

    file_size_mib: float
    row_count: int
    columns: list[str]


# =============================================================================
# Ingestion decision
# =============================================================================


class IngestionDecision(StrictModel):
    """Outcome of the pre-download change-detection check.

    Attributes
    ----------
    should_ingest:
        ``True`` if the remote file has changed and ingestion should proceed.
    is_healing:
        ``True`` if this run is a healing re-ingestion (e.g. previous run failed
        mid-pipeline and local state is inconsistent).
    remote_metadata:
        HTTP HEAD metadata used for the decision.
    """

    should_ingest: bool
    is_healing: bool
    remote_metadata: RemoteFileMetadata


# =============================================================================
# Stage results (composition)
# =============================================================================


class DownloadStageResult(StrictModel):
    """Result of the download (+ optional extraction) stage.

    Attributes
    ----------
    version:
        Run version string (e.g. ``"2026-01-17"``).
    remote_metadata:
        HTTP HEAD metadata captured before download.
    download_info:
        Downloaded file path, hash, and size.
    extraction_info:
        Extraction details if the source format is an archive, ``None`` otherwise.
    """

    version: str
    remote_metadata: RemoteFileMetadata
    download_info: HttpDownloadInfo
    extraction_info: ExtractionInfo | None = None

    @property
    def landing_path(self) -> Path:
        """Path to the file that enters the bronze stage.

        Returns the extracted file when the source is an archive,
        otherwise the raw downloaded file.
        """
        if self.extraction_info is not None:
            return self.extraction_info.file_path
        return self.download_info.path


class BronzeStageResult(StrictModel):
    """Result of the bronze conversion stage.

    Attributes
    ----------
    download:
        Upstream download/extraction context.
    bronze:
        Metrics of the produced bronze parquet file.
    """

    download: DownloadStageResult
    bronze: BronzeInfo


class SilverStageResult(StrictModel):
    """Result of the silver transformation stage.

    Attributes
    ----------
    download:
        Upstream download/extraction context.
    bronze:
        Metrics of the bronze parquet file that was consumed.
    silver:
        Metrics of the produced silver parquet file.
    """

    download: DownloadStageResult
    bronze: BronzeInfo
    silver: SilverInfo


class GoldStageResult(StrictModel):
    """Result of the gold aggregation stage (derived datasets).

    Attributes
    ----------
    file_size_mib:
        Size of the produced gold parquet file.
    row_count:
        Number of rows in the gold table.
    columns:
        Column names in the gold table.
    """

    file_size_mib: float
    row_count: int
    columns: list[str]


# =============================================================================
# Full pipeline snapshot (flattened view for observability / metadata store)
# =============================================================================


class PipelineRunSnapshot(StrictModel):
    """Flattened snapshot of a complete pipeline run for observability.

    Built from a ``SilverStageResult`` to persist in a metadata store
    (Airflow's Postgres instance, local JSON, etc.) without nested models.

    Attributes
    ----------
    version:
        Run version string.
    remote_metadata:
        HTTP HEAD metadata captured before download.
    raw_file_sha256:
        SHA-256 hash of the downloaded file.
    raw_file_size_mib:
        Size of the downloaded file in MiB.
    extracted_file_sha256:
        SHA-256 hash of the extracted file (``None`` if no extraction).
    extracted_file_size_mib:
        Size of the extracted file in MiB (``None`` if no extraction).
    bronze_file_size_mib:
        Size of the bronze parquet file in MiB.
    bronze_row_count:
        Number of rows in the bronze table.
    bronze_columns:
        Column names in the bronze table.
    silver_file_size_mib:
        Size of the silver parquet file in MiB.
    silver_row_count:
        Number of rows in the silver table.
    silver_columns:
        Column names in the silver table.
    """

    # TODO: réutiliser les classes ou tout garder à plat ?

    # Download
    version: str
    remote_metadata: RemoteFileMetadata
    raw_file_sha256: str
    raw_file_size_mib: float
    # Extraction (optional)
    extracted_file_sha256: str | None = None
    extracted_file_size_mib: float | None = None
    # Bronze
    bronze_file_size_mib: float
    bronze_row_count: int
    bronze_columns: list[str]
    # Silver
    silver_file_size_mib: float
    silver_row_count: int
    silver_columns: list[str]
