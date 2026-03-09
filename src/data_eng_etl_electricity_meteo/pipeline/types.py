"""Typed metrics and pipeline context for the medallion architecture stages.

Each pipeline stage produces a ``...Metrics`` type capturing its own output.
All metrics accumulate in a single ``PipelineContext`` passed via Airflow XCom, growing
at each stage: download → (extraction) → bronze → silver.

Architecture
------------

Pipeline functions (typed) ↔ Airflow tasks (dicts via XCom):

    download()  → PipelineContext(download=DownloadMetrics(...))
    extract()   → PipelineContext(download=DownloadMetrics(..., extraction_info=...))
    to_bronze() → PipelineContext(..., bronze=BronzeMetrics(...))
    to_silver() → PipelineContext(..., silver=SilverMetrics(...))
"""

from pathlib import Path
from typing import Any, Self

from pydantic import ValidationError

from data_eng_etl_electricity_meteo.core.data_catalog import IngestionMode
from data_eng_etl_electricity_meteo.core.logger import get_logger
from data_eng_etl_electricity_meteo.core.pydantic_base import StrictModel, format_pydantic_errors
from data_eng_etl_electricity_meteo.utils.download import HttpDownloadInfo
from data_eng_etl_electricity_meteo.utils.remote_metadata import RemoteFileMetadata

logger = get_logger("pipeline")


# --------------------------------------------------------------------------------------
# Stage-specific metrics
# --------------------------------------------------------------------------------------


class ExtractionInfo(StrictModel):
    """Archive extraction details for pipeline traceability.

    Created by ``RemoteIngestionPipeline.extract_archive`` from the
    ``ExtractedFileInfo`` returned by ``extract_7z`` plus archive context.

    Attributes
    ----------
    archive_path
        Path to the source archive.
    file_path
        Path to the extracted file.
    file_hash
        Content hash of the extracted file (algorithm from settings).
    size_mib
        Size of the extracted file in MiB.
    """

    archive_path: Path
    file_path: Path
    file_hash: str
    size_mib: float


class DownloadMetrics(StrictModel):
    """Metrics produced by the download (+ optional extraction) stage.

    Attributes
    ----------
    remote_metadata
        HTTP HEAD metadata captured before download.
    download_info
        Downloaded file path, hash, and size.
    extraction_info
        Extraction details if the source format is an archive, ``None`` otherwise.
    """

    remote_metadata: RemoteFileMetadata
    download_info: HttpDownloadInfo
    extraction_info: ExtractionInfo | None = None

    @property
    def landing_path(self) -> Path:
        """Path of the file entering the bronze stage.

        Returns the extracted file when the source is an archive, otherwise the raw
        downloaded file.
        """
        if self.extraction_info is not None:
            return self.extraction_info.file_path
        return self.download_info.path


class BronzeMetrics(StrictModel):
    """Metrics produced by the bronze conversion stage."""

    file_size_mib: float
    rows_count: int
    columns: list[str]


class IncrementalDiffMetrics(StrictModel):
    """Delta statistics computed during silver transformation for incremental datasets.

    Attributes
    ----------
    rows_total
        Total rows in the full silver snapshot.
    rows_added
        Rows whose PK is absent from the previous snapshot (inserts).
    rows_changed
        Rows whose PK exists but at least one non-key column differs (updates).
    rows_unchanged
        Rows identical to the previous snapshot (skipped).
    """

    rows_total: int
    rows_added: int
    rows_changed: int
    rows_unchanged: int


class SilverMetrics(StrictModel):
    """Metrics produced by the silver transformation stage.

    Attributes
    ----------
    file_size_mib
        Size of ``current.parquet`` in MiB.
    rows_count
        Number of rows in the silver snapshot.
    columns
        Column names.
    diff
        Incremental diff statistics, or ``None`` for snapshot datasets.
    """

    file_size_mib: float
    rows_count: int
    columns: list[str]
    diff: IncrementalDiffMetrics | None = None


class GoldMetrics(StrictModel):
    """Metrics produced by the gold aggregation stage (dbt in Postgres)."""

    table: str
    rows_count: int
    columns: list[str]


class LoadPostgresMetrics(StrictModel):
    """Metrics produced by a silver → Postgres load operation.

    Attributes
    ----------
    table
        Schema-qualified table name (e.g. ``silver.dim_installations``).
    mode
        Ingestion mode: ``"snapshot"`` or ``"incremental"``.
    rows_loaded
        Number of rows written (for incremental: rows inserted + updated).
    diff
        Incremental diff statistics from the silver stage, or ``None``.
    """

    table: str
    mode: IngestionMode
    rows_loaded: int
    diff: IncrementalDiffMetrics | None = None


# --------------------------------------------------------------------------------------
# Ingestion decision
# --------------------------------------------------------------------------------------


class IngestionDecision(StrictModel):
    """Outcome of the pre-download change-detection check.

    Attributes
    ----------
    should_ingest
        ``True`` if the remote file has changed and ingestion should proceed.
    is_healing
        ``True`` if this run is a healing re-ingestion
        (e.g. previous run failed mid-pipeline and local state is inconsistent).
    remote_metadata
        HTTP HEAD metadata used for the decision.
    """

    should_ingest: bool
    is_healing: bool
    remote_metadata: RemoteFileMetadata


# --------------------------------------------------------------------------------------
# Pipeline context (accumulates across stages via XCom)
# --------------------------------------------------------------------------------------


class PipelineContext(StrictModel):
    """Pipeline state passed via Airflow XCom, accumulating metrics across stages.

    Starts with only ``download`` populated after the ingest/extract tasks, then
    ``bronze`` and ``silver`` are added by their respective tasks.

    Attributes
    ----------
    version
        Run version string (e.g. ``"2026-01-17"``). Determined by the DAG from the run
        date and ingestion frequency, before any stage executes.
    is_healing
        ``True`` if this run is a healing re-ingestion (inconsistent local state).
        When set, downstream stages skip content-hash smart-skip.
    download
        Metrics from the download (and optional extraction) stage.
    bronze
        Metrics from the bronze conversion stage, or ``None`` if not yet run.
    silver
        Metrics from the silver transformation stage, or ``None`` if not yet run.
    """

    version: str
    is_healing: bool = False
    download: DownloadMetrics
    bronze: BronzeMetrics | None = None
    silver: SilverMetrics | None = None


# --------------------------------------------------------------------------------------
# Pipeline run snapshot (nested view for observability / metadata store)
# --------------------------------------------------------------------------------------


class DownloadSnapshot(StrictModel):
    """Persisted download metrics, without ephemeral file paths.

    Attributes
    ----------
    remote_metadata
        HTTP HEAD metadata captured before download.
    file_hash
        Content hash of the downloaded file (algorithm from settings).
    size_mib
        Size of the downloaded file in MiB.
    """

    remote_metadata: RemoteFileMetadata
    file_hash: str
    size_mib: float


class ExtractionSnapshot(StrictModel):
    """Persisted extraction metrics, without ephemeral file paths.

    Attributes
    ----------
    file_hash
        Content hash of the extracted file (algorithm from settings).
    size_mib
        Size of the extracted file in MiB.
    """

    file_hash: str
    size_mib: float


class PipelineRunSnapshot(StrictModel):
    """Snapshot of a complete pipeline run for observability.

    Persisted in Airflow's Asset metadata after each successful silver run, and read
    back on subsequent runs for smart-skip decisions.

    File paths are excluded: they are deterministic from ``version`` + ``dataset_name``
    via ``RemotePathResolver``, and ephemeral landing paths no longer exist after the
    bronze stage.

    Attributes
    ----------
    version
        Run version string.
    download
        Download metrics without ephemeral file paths.
    extraction
        Extraction metrics, or ``None`` if the source is not an archive.
    bronze
        Bronze conversion metrics (required — only set for complete runs).
    silver
        Silver transformation metrics (required — only set for complete runs).
    """

    version: str
    download: DownloadSnapshot
    extraction: ExtractionSnapshot | None = None
    bronze: BronzeMetrics
    silver: SilverMetrics

    @classmethod
    def from_context(cls, context: PipelineContext) -> Self:
        """Build a run snapshot from a completed pipeline context.

        Parameters
        ----------
        context
            Complete pipeline context with all stages populated.

        Returns
        -------
        PipelineRunSnapshot
            Snapshot suitable for persistence in a metadata store.

        Raises
        ------
        ValueError
            If bronze or silver metrics are missing from the context.
        """
        if context.bronze is None or context.silver is None:
            raise ValueError(
                "Cannot build snapshot: pipeline context is incomplete "
                f"(bronze={'set' if context.bronze else 'missing'}, "
                f"silver={'set' if context.silver else 'missing'})"
            )

        dl = context.download
        extraction = dl.extraction_info

        return cls(
            version=context.version,
            download=DownloadSnapshot(
                remote_metadata=dl.remote_metadata,
                file_hash=dl.download_info.file_hash,
                size_mib=dl.download_info.size_mib,
            ),
            extraction=ExtractionSnapshot(
                file_hash=extraction.file_hash,
                size_mib=extraction.size_mib,
            )
            if extraction
            else None,
            bronze=context.bronze,
            silver=context.silver,
        )

    @classmethod
    def from_metadata_dict(cls, metadata: dict[str, Any] | None) -> Self | None:
        """Parse and validate previous run metadata dict.

        Parameters
        ----------
        metadata
            Raw metadata dict (e.g. from Airflow XCom ``extra``), or ``None``.

        Returns
        -------
        PipelineRunSnapshot | None
            Validated snapshot, or ``None`` if missing or corrupted.
        """
        if not metadata:
            return None
        try:
            return cls.model_validate(metadata)
        except ValidationError as e:
            logger.warning("Previous run metadata corrupted", **format_pydantic_errors(e))
            return None
