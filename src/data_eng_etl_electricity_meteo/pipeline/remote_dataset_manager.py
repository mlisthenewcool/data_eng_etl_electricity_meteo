"""Pipeline manager for remote (HTTP) datasets.

Orchestrates the full ingestion pipeline for a single remote dataset,
completely decoupled from Airflow:

1. **Ingest** — HEAD check + smart skip + download to landing
2. **Extract** — 7z extraction with SHA-256 integrity (optional)
3. **Bronze** — landing → versioned Parquet + ``latest`` symlink
4. **Silver** — bronze latest → business transform → ``current`` / ``backup``

Uses ``RemotePathResolver`` for all path operations and
``RemoteFileManager`` for symlinks, rotation, and rollback.
"""

import shutil
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Any

import duckdb
import httpx

from data_eng_etl_electricity_meteo.core.data_catalog import RemoteDatasetConfig
from data_eng_etl_electricity_meteo.core.exceptions import (
    ArchiveNotFoundError,
    BronzeStageError,
    ExtractStageError,
    FileIntegrityError,
    FileNotFoundInArchiveError,
    IngestStageError,
    SilverStageError,
)
from data_eng_etl_electricity_meteo.core.logger import get_logger
from data_eng_etl_electricity_meteo.pipeline.file_manager import RemoteFileManager
from data_eng_etl_electricity_meteo.pipeline.path_resolver import RemotePathResolver
from data_eng_etl_electricity_meteo.pipeline.stage_types import (
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
from data_eng_etl_electricity_meteo.utils.download import download_to_file
from data_eng_etl_electricity_meteo.utils.extraction import extract_7z
from data_eng_etl_electricity_meteo.utils.remote_metadata import (
    RemoteFileMetadata,
    get_remote_file_metadata,
    has_remote_file_changed,
)

logger = get_logger("pipeline")


@dataclass
class RemoteDatasetPipeline:
    """Pipeline manager for a single remote dataset.

    Orchestrates: ingest → (extract) → to_bronze → to_silver.

    Attributes
    ----------
    dataset:
        Remote dataset configuration from the catalog.

    Raises
    ------
    TransformNotFoundError
        At construction time if bronze or silver transforms are not registered.
    """

    dataset: RemoteDatasetConfig

    def __post_init__(self) -> None:
        """Validate that transform modules exist for this dataset (fast-fail)."""
        get_bronze_transform(self.dataset.name)
        get_silver_transform(self.dataset.name)

    @cached_property
    def resolver(self) -> RemotePathResolver:
        """Path resolver for this dataset (created once, cached)."""
        return RemotePathResolver(dataset_name=self.dataset.name)

    # ========================================
    # INGESTION
    # ========================================

    def _evaluate_ingestion_need(
        self,
        current_remote_file_info: RemoteFileMetadata,
        previous_remote_file_info: RemoteFileMetadata | None,
    ) -> IngestionDecision:
        """Determine if ingestion is required based on remote metadata and local state.

        Checks upstream consistency (silver file vs metadata existence) and
        compares remote metadata to detect changes.

        Parameters
        ----------
        current_remote_file_info:
            Freshly fetched HTTP HEAD metadata.
        previous_remote_file_info:
            Metadata from the previous successful run, or ``None``.

        Returns
        -------
        IngestionDecision
            Whether to ingest, whether this is a healing run, and the
            remote metadata used for the decision.
        """
        # --- Check upstream consistency ---
        silver_path = self.resolver.silver_current_path
        silver_file_exists = silver_path.exists()
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

        # --- Smart skip #1 : Remote metadata comparison ---
        if previous_remote_file_info and silver_file_exists:
            if not has_remote_file_changed(
                current=current_remote_file_info, previous=previous_remote_file_info
            ):
                logger.info("Skipping ingestion: remote metadata unchanged")
                return IngestionDecision(
                    should_ingest=False, is_healing=False, remote_metadata=current_remote_file_info
                )

        return IngestionDecision(
            should_ingest=True, is_healing=False, remote_metadata=current_remote_file_info
        )

    def _download(self, version: str, remote_metadata: RemoteFileMetadata) -> PipelineContext:
        """Download source file to the landing directory.

        Parameters
        ----------
        version:
            Run version string used as fallback filename.
        remote_metadata:
            HTTP HEAD metadata captured before download.

        Returns
        -------
        PipelineContext
            Initial pipeline context with download metrics populated.
        """
        download_info = download_to_file(
            url=self.dataset.source.url_as_str,
            dest_dir=self.resolver.landing_dir,
            fallback_filename=f"{version}.{self.dataset.source.format.value}",
        )

        return PipelineContext(
            version=version,
            download=DownloadMetrics(
                remote_metadata=remote_metadata,
                download_info=download_info,
            ),
        )

    def ingest(
        self, version: str, previous_metadata: dict[str, Any] | None
    ) -> PipelineContext | None:
        """Run the full ingestion flow: smart-skip checks then download.

        Returns ``None`` if ingestion was skipped (unchanged remote or
        identical content hash), otherwise the initial pipeline context.

        Parameters
        ----------
        version:
            Run version string (e.g. ``"2026-01-17"``).
        previous_metadata:
            Raw metadata dict from the previous run, or ``None``.

        Returns
        -------
        PipelineContext | None
            Initial pipeline context, or ``None`` if skipped.
        """
        try:
            logger.info("Ingesting", version=version)

            # --- 1. Load metadata from previous run ---
            safe_previous_metadata = PipelineRunSnapshot.from_metadata_dict(previous_metadata)

            # --- 2. Retrieve metadata from remote with HEAD request ---
            remote_file_info = get_remote_file_metadata(url=self.dataset.source.url_as_str)
            previous_remote_metadata = (
                safe_previous_metadata.download.remote_metadata if safe_previous_metadata else None
            )

            # --- 3. Evaluate whether ingestion is needed ---
            need = self._evaluate_ingestion_need(
                current_remote_file_info=remote_file_info,
                previous_remote_file_info=previous_remote_metadata,
            )

            if not need.should_ingest:
                return None

            # --- 4. Download content ---
            context = self._download(version=version, remote_metadata=remote_file_info)

            # --- 5. Hash check (smart skip #2) but don't skip in healing mode ---
            if not need.is_healing and safe_previous_metadata:
                if self._should_skip_on_hash(
                    previous_hash=safe_previous_metadata.download.file_hash,
                    current_hash=context.download.download_info.file_hash,
                ):
                    return None

            return context

        except httpx.HTTPError as err:
            raise IngestStageError(self.dataset.name) from err

    def should_skip_extraction(
        self, context: PipelineContext, previous_metadata: dict[str, Any] | None
    ) -> bool:
        """Check if extracted file hash matches previous run (smart skip).

        Parameters
        ----------
        context:
            Pipeline context with ``download.extraction_info`` populated.
        previous_metadata:
            Raw metadata dict from the previous run, or ``None``.

        Returns
        -------
        bool
            ``True`` if extraction output is identical and processing can be skipped.
        """
        safe_previous_metadata = PipelineRunSnapshot.from_metadata_dict(previous_metadata)
        if not safe_previous_metadata or not safe_previous_metadata.extraction:
            return False

        if context.download.extraction_info is None:
            return False

        return self._should_skip_on_hash(
            safe_previous_metadata.extraction.file_hash,
            context.download.extraction_info.file_hash,
        )

    def extract_archive(self, context: PipelineContext) -> PipelineContext:
        """Extract target file from archive and populate ``extraction_info``.

        Parameters
        ----------
        context:
            Pipeline context whose ``download.download_info.path`` points to the
            archive.

        Returns
        -------
        PipelineContext
            Updated context with ``download.extraction_info`` populated.
        """
        try:
            archive_path = context.download.download_info.path
            landing_dir = archive_path.parent

            # inner_file guaranteed by RemoteSourceConfig validator but ty complains
            # NOTE: Local variable enables ty to narrow str | None → str after the check
            inner_file = self.dataset.source.inner_file
            if inner_file is None:
                raise ExtractStageError(self.dataset.name) from ValueError(
                    "inner_file is None (should be guaranteed by RemoteSourceConfig validator)"
                )

            extract_info = extract_7z(
                archive_path=archive_path,
                target_filename=inner_file,
                dest_dir=landing_dir,
                validate_sqlite=Path(inner_file).suffix == ".gpkg",
            )

            return PipelineContext(
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
        except (
            ArchiveNotFoundError,
            FileNotFoundInArchiveError,
            FileIntegrityError,
        ) as err:
            raise ExtractStageError(self.dataset.name) from err

    # =============================================================================
    # Smart skip #2
    # =============================================================================

    def _should_skip_on_hash(self, previous_hash: str | None, current_hash: str) -> bool:
        """Compare content hashes; cleanup landing and return ``True`` if identical.

        Parameters
        ----------
        previous_hash:
            Hash from the previous successful run, or ``None``.
        current_hash:
            Hash of the current file.

        Returns
        -------
        bool
            ``True`` if hashes match (processing can be skipped).
        """
        if previous_hash == current_hash:
            logger.info("Skipping: content hash identical to previous run")
            self._cleanup_landing()
            return True

        return False

    def _cleanup_landing(self) -> None:
        """Remove the landing directory and all its contents."""
        if self.resolver.landing_dir.exists() and self.resolver.landing_dir.is_dir():
            shutil.rmtree(self.resolver.landing_dir)

    # =============================================================================
    # Transformations
    # =============================================================================

    def to_bronze(self, context: PipelineContext) -> PipelineContext:
        """Convert landing file to versioned bronze Parquet.

        Steps: read landing → apply transform → write versioned parquet →
        update ``latest.parquet`` symlink → cleanup landing.

        Parameters
        ----------
        context:
            Pipeline context from the ingest/extract stage.

        Returns
        -------
        PipelineContext
            Updated context with ``bronze`` metrics populated.
        """
        try:
            bronze_path = self.resolver.bronze_path(context.version)
            bronze_path.parent.mkdir(parents=True, exist_ok=True)

            logger.info("Converting to bronze", version=context.version)

            # Retrieve and apply dataset-specific bronze transformation
            transform = get_bronze_transform(self.dataset.name)
            df = transform(context.download.landing_path)
            df.write_parquet(bronze_path)

            # Update latest symlink to point to this version
            file_manager = RemoteFileManager(self.resolver)
            file_manager.update_bronze_latest_link(context.version)

            # Cleanup all landing files
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
        except (duckdb.Error, OSError) as err:
            raise BronzeStageError(self.dataset.name) from err

    def to_silver(self, context: PipelineContext) -> PipelineContext:
        """Apply business transformations to create silver layer.

        Steps: rotate silver (current → backup) → read bronze latest →
        apply transform → write ``current.parquet``.

        Parameters
        ----------
        context:
            Pipeline context from the bronze stage.

        Returns
        -------
        PipelineContext
            Updated context with ``silver`` metrics populated.
        """
        try:
            logger.info("Transforming to silver", version=context.version)

            # Retrieve and apply dataset-specific silver transformation
            transform = get_silver_transform(self.dataset.name)
            df = transform(self.resolver.bronze_latest_path)

            # Rotate silver BEFORE writing to disk: current → backup
            file_manager = RemoteFileManager(self.resolver)
            file_manager.rotate_silver()

            self.resolver.silver_current_path.parent.mkdir(parents=True, exist_ok=True)

            # Now that we rotated versions, write to disk
            df.write_parquet(self.resolver.silver_current_path)

            columns = df.columns
            row_count = len(df)
            parquet_size = round(
                self.resolver.silver_current_path.stat().st_size / (1024 * 1024), 2
            )

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
        except (duckdb.Error, OSError) as err:
            raise SilverStageError(self.dataset.name) from err
