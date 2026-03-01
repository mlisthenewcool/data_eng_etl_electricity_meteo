"""Pipeline manager for remote (HTTP) datasets.

Orchestrates the full ingestion pipeline for a single remote dataset, completely
decoupled from Airflow:

1. **Download** — HEAD check + smart skip + download to landing
2. **Extract** — 7z extraction with SHA-256 integrity (optional)
3. **Bronze** — landing → versioned Parquet + ``latest`` symlink
4. **Silver** — bronze latest → business transform → ``current`` / ``backup``

The download step can be customized via ``custom_download``: a callable that takes a
landing directory and returns the path to the downloaded file.
When set, the standard HEAD check + smart skip + single-URL download is replaced by the
custom logic (e.g. multi-file merge for climatologie).

Uses ``RemotePathResolver`` for all path operations and ``RemoteFileManager`` for
symlinks, rotation, and rollback.
"""

import shutil
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import cached_property
from pathlib import Path

import duckdb
import httpx

from data_eng_etl_electricity_meteo.core.data_catalog import RemoteDatasetConfig
from data_eng_etl_electricity_meteo.core.exceptions import (
    BronzeStageError,
    DownloadStageError,
    ExtractionError,
    ExtractStageError,
    SilverStageError,
    TransformValidationError,
)
from data_eng_etl_electricity_meteo.core.logger import get_logger
from data_eng_etl_electricity_meteo.core.settings import settings
from data_eng_etl_electricity_meteo.pipeline.file_manager import RemoteFileManager
from data_eng_etl_electricity_meteo.pipeline.path_resolver import RemotePathResolver
from data_eng_etl_electricity_meteo.pipeline.progress import (
    AirflowDownloadProgress,
    AirflowExtractProgress,
)
from data_eng_etl_electricity_meteo.pipeline.types import (
    BronzeMetrics,
    DownloadMetrics,
    ExtractionInfo,
    IngestionDecision,
    PipelineContext,
    PipelineRunSnapshot,
    SilverMetrics,
)
from data_eng_etl_electricity_meteo.transformations.registry import (
    get_bronze_transform,
    get_silver_transform,
)
from data_eng_etl_electricity_meteo.utils.download import HttpDownloadInfo, download_to_file
from data_eng_etl_electricity_meteo.utils.extraction import extract_7z
from data_eng_etl_electricity_meteo.utils.file_hash import FileHasher
from data_eng_etl_electricity_meteo.utils.remote_metadata import (
    RemoteFileMetadata,
    get_remote_file_metadata,
)

logger = get_logger("pipeline")

# Callable that takes a landing directory and returns the path to
# the downloaded file. Used to inject custom download logic (e.g.
# multi-file merge for climatologie) instead of the standard
# single-URL download.
CustomDownloadFunc = Callable[[Path], Path]


@dataclass
class RemoteIngestionPipeline:
    """Pipeline manager for a single remote dataset.

    Orchestrates: download → (extract) → to_bronze → to_silver.

    Attributes
    ----------
    dataset
        Remote dataset configuration from the catalog.
    custom_download
        Optional download strategy. When set, replaces the standard HEAD check +
        single-URL download with custom logic. The callable receives the landing
        directory and must return the path to the produced file.

    Raises
    ------
    TransformNotFoundError
        At construction time if bronze or silver transforms are not registered.
    """

    dataset: RemoteDatasetConfig
    custom_download: CustomDownloadFunc | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        """Validate that transform modules exist for this dataset (fast-fail)."""
        get_bronze_transform(self.dataset.name)
        get_silver_transform(self.dataset.name)

    @cached_property
    def resolver(self) -> RemotePathResolver:
        """Path resolver for this dataset (created once, cached)."""
        return RemotePathResolver(dataset_name=self.dataset.name)

    @cached_property
    def _file_manager(self) -> RemoteFileManager:
        """File manager for symlinks, rotation and rollback (created once, cached)."""
        return RemoteFileManager(self.resolver)

    # ---------------------------------------------------------------------------
    # Download
    # ---------------------------------------------------------------------------

    def _decide_ingestion(
        self,
        current_remote_file_info: RemoteFileMetadata,
        previous_remote_file_info: RemoteFileMetadata | None,
    ) -> IngestionDecision:
        """Determine if ingestion is required based on remote metadata and local state.

        Checks upstream consistency (silver file vs metadata existence) and compares
        remote metadata to detect changes.

        Parameters
        ----------
        current_remote_file_info
            Freshly fetched HTTP HEAD metadata.
        previous_remote_file_info
            Metadata from the previous successful run, or ``None``.

        Returns
        -------
        IngestionDecision
            Whether to ingest, whether this is a healing run, and the remote metadata
            used for the decision.
        """
        silver_file_exists = self.resolver.silver_current_path.exists()
        remote_metadata_exists = previous_remote_file_info is not None

        if remote_metadata_exists != silver_file_exists:
            logger.warning(
                "Inconsistent state, forcing ingestion to heal",
                silver_file_exists=silver_file_exists,
                remote_metadata_exists=remote_metadata_exists,
            )
            return IngestionDecision(
                should_ingest=True, is_healing=True, remote_metadata=current_remote_file_info
            )

        # After the guard above, remote_metadata_exists == silver_file_exists.
        # Checking previous_remote_file_info alone is sufficient.
        # TODO: add If-None-Match header support for HTTP 304 responses
        if previous_remote_file_info:
            change_detection_result = current_remote_file_info.compare_with(
                previous_remote_file_info
            )
            if not change_detection_result.has_changed:
                logger.info(
                    "Skipping ingestion: remote metadata unchanged",
                    reason=change_detection_result.reason,
                )
                return IngestionDecision(
                    should_ingest=False, is_healing=False, remote_metadata=current_remote_file_info
                )

        return IngestionDecision(
            should_ingest=True, is_healing=False, remote_metadata=current_remote_file_info
        )

    def download(
        self, version: str, previous_snapshot: PipelineRunSnapshot | None
    ) -> PipelineContext | None:
        """Run the full ingestion flow: smart-skip checks then download.

        When ``custom_download`` is set, delegates entirely to the custom callable
        (no HEAD check, no smart skip).
        Otherwise, performs the standard HEAD check + smart skip + single-URL download.

        Returns ``None`` if ingestion was skipped (unchanged remote or identical content
        hash), otherwise the initial pipeline context.

        Parameters
        ----------
        version
            Run version string (e.g. ``"2026-01-17"``).
        previous_snapshot
            Validated snapshot from the previous run, or ``None``.

        Returns
        -------
        PipelineContext | None
            Initial pipeline context, or ``None`` if skipped.

        Raises
        ------
        DownloadStageError
            On any HTTP failure (connect, timeout, status error) during the HEAD check
            or the download, or if the custom download fails.
        """
        if self.custom_download is not None:
            return self._run_custom_download(version)

        return self._run_standard_download(version, previous_snapshot)

    def _run_custom_download(self, version: str) -> PipelineContext:
        """Execute the custom download strategy.

        Parameters
        ----------
        version
            Run version string.

        Returns
        -------
        PipelineContext
            Pipeline context with the landing file path.

        Raises
        ------
        DownloadStageError
            If the custom download callable fails.
        """
        assert self.custom_download is not None  # narrowing for type checker

        logger.info("Running custom download", version=version)

        try:
            landing_path = self.custom_download(self.resolver.landing_dir)
        except Exception as err:
            raise DownloadStageError("Custom download failed") from err

        file_hash = FileHasher.hash_file(landing_path)
        size_mib = round(landing_path.stat().st_size / (1024 * 1024), 2)

        logger.info(
            "Custom download complete",
            landing_path=landing_path,
            size_mib=size_mib,
        )

        return PipelineContext(
            version=version,
            download=DownloadMetrics(
                remote_metadata=RemoteFileMetadata(),
                download_info=HttpDownloadInfo(
                    path=landing_path,
                    file_hash=file_hash,
                    size_mib=size_mib,
                ),
            ),
        )

    def _run_standard_download(
        self, version: str, previous_snapshot: PipelineRunSnapshot | None
    ) -> PipelineContext | None:
        """Execute the standard single-URL download with smart-skip logic.

        Parameters
        ----------
        version
            Run version string (e.g. ``"2026-01-17"``).
        previous_snapshot
            Validated snapshot from the previous run, or ``None``.

        Returns
        -------
        PipelineContext | None
            Initial pipeline context, or ``None`` if skipped.

        Raises
        ------
        DownloadStageError
            On any HTTP failure.
        """
        logger.info("Downloading", version=version)

        # --- 1. Fetch current remote metadata (HEAD request) ---
        try:
            remote_file_info = get_remote_file_metadata(url=self.dataset.source.url_as_str)
        except httpx.HTTPError as err:
            raise DownloadStageError() from err

        previous_remote_metadata = (
            previous_snapshot.download.remote_metadata if previous_snapshot else None
        )

        # --- 2. Smart skip #1: remote metadata comparison ---
        ingestion_decision = self._decide_ingestion(
            current_remote_file_info=remote_file_info,
            previous_remote_file_info=previous_remote_metadata,
        )

        if not ingestion_decision.should_ingest:
            return None

        # --- 3. Download ---
        try:
            download_info = download_to_file(
                url=self.dataset.source.url_as_str,
                dest_dir=self.resolver.landing_dir,
                fallback_filename=f"{version}.{self.dataset.source.format.value}",
                progress=AirflowDownloadProgress if settings.is_running_on_airflow else None,
            )
        except httpx.HTTPError as err:
            raise DownloadStageError() from err

        context = PipelineContext(
            version=version,
            download=DownloadMetrics(
                remote_metadata=remote_file_info,
                download_info=download_info,
            ),
        )

        # --- 4. Smart skip #2: content hash comparison (skipped in healing mode) ---
        if not ingestion_decision.is_healing and previous_snapshot:
            if self._should_skip_on_hash(
                previous_hash=previous_snapshot.download.file_hash,
                current_hash=context.download.download_info.file_hash,
            ):
                self._cleanup_landing()
                return None

        return context

    # ---------------------------------------------------------------------------
    # Extraction
    # ---------------------------------------------------------------------------

    def extract_archive(
        self, context: PipelineContext, previous_snapshot: PipelineRunSnapshot | None
    ) -> PipelineContext | None:
        """Extract target file from archive; returns ``None`` if hash unchanged.

        Parameters
        ----------
        context
            Pipeline context whose ``download.download_info.path`` points to the
            archive.
        previous_snapshot
            Validated snapshot from the previous run, or ``None``.

        Returns
        -------
        PipelineContext | None
            Updated context with ``download.extraction_info`` populated, or ``None`` if
            the extracted content hash matches the previous run.

        Raises
        ------
        ExtractStageError
            On archive not found, missing inner file, integrity failure, or if
            ``inner_file`` is ``None``
            (should not happen — guaranteed by ``RemoteSourceConfig`` validator).
        """
        archive_path = context.download.download_info.path
        landing_dir = archive_path.parent

        # inner_file is guaranteed non-None by RemoteSourceConfig's validator for
        # archive formats, but ty cannot prove it here. The check guards against
        # any future regression and also narrows the type for the call below.
        inner_file = self.dataset.source.inner_file
        if inner_file is None:
            raise ExtractStageError(
                "inner_file required for archive dataset", dataset=self.dataset.name
            )

        try:
            extract_info = extract_7z(
                archive_path=archive_path,
                target_filename=inner_file,
                dest_dir=landing_dir,
                validate_sqlite=Path(inner_file).suffix == ".gpkg",
                progress=AirflowExtractProgress if settings.is_running_on_airflow else None,
            )
        except ExtractionError as err:
            raise ExtractStageError() from err

        updated_context = PipelineContext(
            version=context.version,
            download=DownloadMetrics(
                remote_metadata=context.download.remote_metadata,
                download_info=context.download.download_info,
                extraction_info=ExtractionInfo(
                    archive_path=archive_path,
                    file_path=extract_info.path,
                    file_hash=extract_info.file_hash,
                    size_mib=extract_info.size_mib,
                ),
            ),
        )

        # Smart skip: hash comparison against previous extraction
        if previous_snapshot and previous_snapshot.extraction:
            if self._should_skip_on_hash(
                previous_hash=previous_snapshot.extraction.file_hash,
                current_hash=extract_info.file_hash,
            ):
                self._cleanup_landing()
                return None

        return updated_context

    # ---------------------------------------------------------------------------
    # Helpers
    # ---------------------------------------------------------------------------

    def _should_skip_on_hash(self, previous_hash: str | None, current_hash: str) -> bool:
        """Return ``True`` if hashes match (content unchanged since last run)."""
        if previous_hash == current_hash:
            logger.info("Skipping: content hash identical to previous run")
            return True
        return False

    def _cleanup_landing(self) -> None:
        """Remove the landing directory and all its contents."""
        if self.resolver.landing_dir.exists() and self.resolver.landing_dir.is_dir():
            shutil.rmtree(self.resolver.landing_dir)

    # ---------------------------------------------------------------------------
    # Transformations
    # ---------------------------------------------------------------------------

    def to_bronze(self, context: PipelineContext) -> PipelineContext:
        """Convert landing file to versioned bronze Parquet.

        Steps: read landing → apply transform → write versioned parquet → update
        ``latest.parquet`` symlink → cleanup landing.

        Parameters
        ----------
        context
            Pipeline context from the ingest/extract stage.

        Returns
        -------
        PipelineContext
            Updated context with ``bronze`` metrics populated.

        Raises
        ------
        BronzeStageError
            On transform failure (DuckDB error), I/O error writing the parquet, or
            failure updating the ``latest`` symlink.
        """
        bronze_path = self.resolver.bronze_path(context.version)
        bronze_path.parent.mkdir(parents=True, exist_ok=True)

        logger.info("Converting to bronze", version=context.version)

        # TransformNotFoundError propagates directly
        # (programming error, fast-fail by __post_init__)
        transform = get_bronze_transform(self.dataset.name)

        try:
            df = transform(context.download.landing_path)
            df.write_parquet(bronze_path)
        except (duckdb.Error, OSError) as err:
            raise BronzeStageError() from err

        try:
            self._file_manager.update_bronze_latest_link(context.version)
        except OSError as err:
            raise BronzeStageError() from err

        self._cleanup_landing()

        columns = df.columns
        row_count = len(df)
        parquet_size = round(bronze_path.stat().st_size / (1024 * 1024), 2)

        logger.info(
            "Bronze conversion complete",
            row_count=row_count,
            n_columns=len(columns),
            file_size_mib=parquet_size,
        )

        return PipelineContext(
            version=context.version,
            download=context.download,
            bronze=BronzeMetrics(
                file_size_mib=parquet_size,
                row_count=row_count,
                columns=columns,
            ),
        )

    def to_silver(self, context: PipelineContext) -> PipelineContext:
        """Apply business transformations to create silver layer.

        Steps: rotate silver (current → backup) → read bronze latest → apply transform →
        write ``current.parquet``.

        Parameters
        ----------
        context
            Pipeline context from the bronze stage.

        Returns
        -------
        PipelineContext
            Updated context with ``silver`` metrics populated.

        Raises
        ------
        SilverStageError
            On transform failure (DuckDB error, validation error) or I/O error during
            silver file rotation or write.
        """
        logger.info("Transforming to silver", version=context.version)

        # TransformNotFoundError propagates directly
        # (programming error, fast-fail by __post_init__)
        transform = get_silver_transform(self.dataset.name)

        try:
            df = transform(self.resolver.bronze_latest_path)
        except (duckdb.Error, OSError, TransformValidationError) as err:
            raise SilverStageError() from err

        # Rotate silver BEFORE writing to disk: current → backup
        try:
            self._file_manager.rotate_silver()
            self.resolver.silver_current_path.parent.mkdir(parents=True, exist_ok=True)
            df.write_parquet(self.resolver.silver_current_path)
        except OSError as err:
            self._file_manager.rollback_silver()
            raise SilverStageError() from err

        columns = df.columns
        row_count = len(df)
        parquet_size = round(self.resolver.silver_current_path.stat().st_size / (1024 * 1024), 2)

        logger.info(
            "Silver transformation complete",
            row_count=row_count,
            n_columns=len(columns),
            file_size_mib=parquet_size,
        )

        return PipelineContext(
            version=context.version,
            download=context.download,
            bronze=context.bronze,
            silver=SilverMetrics(
                file_size_mib=parquet_size,
                row_count=row_count,
                columns=columns,
            ),
        )
