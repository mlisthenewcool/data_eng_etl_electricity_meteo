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
from pydantic import ValidationError

from data_eng_etl_electricity_meteo.core.data_catalog import RemoteDatasetConfig
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
from data_eng_etl_electricity_meteo.core.logger import get_logger
from data_eng_etl_electricity_meteo.core.pydantic_base import format_pydantic_errors
from data_eng_etl_electricity_meteo.pipeline.file_manager import RemoteFileManager
from data_eng_etl_electricity_meteo.pipeline.path_resolver import RemotePathResolver
from data_eng_etl_electricity_meteo.pipeline.stage_types import (
    BronzeInfo,
    BronzeStageResult,
    DownloadStageResult,
    IngestionDecision,
    PipelineRunSnapshot,
    SilverInfo,
    SilverStageResult,
)
from data_eng_etl_electricity_meteo.transformations.registry import (
    get_bronze_transform,
    get_silver_transform,
)
from data_eng_etl_electricity_meteo.utils.download import download_to_file
from data_eng_etl_electricity_meteo.utils.extraction import ExtractionInfo, extract_7z
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

    @staticmethod
    def _safely_load_metadata(metadata: dict[str, Any] | None) -> PipelineRunSnapshot | None:
        """Parse previous run metadata.

        Parameters
        ----------
        metadata:
            Raw metadata dict (e.g. from Airflow XCom), or ``None``.

        Returns
        -------
        PipelineRunSnapshot | None
            Validated snapshot, or ``None`` if missing or corrupted.
        """
        if not metadata:
            return None
        try:
            return PipelineRunSnapshot.model_validate(metadata)
        except ValidationError as e:
            logger.warning("Metadata corrupted", **format_pydantic_errors(e))
            return None

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
        # --- 1. Check upstream consistency ---
        silver_path = self.resolver.silver_current_path
        silver_file_exists = silver_path.exists()
        remote_metadata_exists = previous_remote_file_info is not None

        if remote_metadata_exists != silver_file_exists:
            logger.warning(
                "Inconsistent state. Forcing ingestion to heal.",
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
                logger.info("Skipping: remote metadata unchanged since previous successful run")
                return IngestionDecision(
                    should_ingest=False, is_healing=False, remote_metadata=current_remote_file_info
                )

        return IngestionDecision(
            should_ingest=True, is_healing=False, remote_metadata=current_remote_file_info
        )

    def _download(self, version: str, remote_metadata: RemoteFileMetadata) -> DownloadStageResult:
        """Download source file to the landing directory.

        Parameters
        ----------
        version:
            Run version string used as fallback filename.
        remote_metadata:
            HTTP HEAD metadata captured before download.

        Returns
        -------
        DownloadStageResult
            Download info with version and remote metadata.
        """
        download_info = download_to_file(
            url=self.dataset.source.url_as_str,
            dest_dir=self.resolver.landing_dir,
            fallback_filename=f"{version}.{self.dataset.source.format.value}",
        )

        return DownloadStageResult(
            version=version, remote_metadata=remote_metadata, download_info=download_info
        )

    def ingest(
        self, version: str, previous_metadata: dict[str, Any] | None
    ) -> DownloadStageResult | bool:
        """Run the full ingestion flow: smart-skip checks then download.

        Returns ``False`` if ingestion was skipped (unchanged remote or
        identical content hash), otherwise the download result.

        Parameters
        ----------
        version:
            Run version string (e.g. ``"2026-01-17"``).
        previous_metadata:
            Raw metadata dict from the previous run, or ``None``.

        Returns
        -------
        DownloadStageResult | bool
            Download result, or ``False`` if skipped.
        """
        try:
            # --- 1. Load metadata from previous run ---
            safe_previous_metadata = self._safely_load_metadata(previous_metadata)

            # --- 2. Retrieve metadata from remote with HEAD request ---
            remote_file_info = get_remote_file_metadata(url=self.dataset.source.url_as_str)
            previous_remote_metadata = (
                safe_previous_metadata.remote_metadata if safe_previous_metadata else None
            )

            # --- 3. Evaluate whether ingestion is needed ---
            need = self._evaluate_ingestion_need(
                current_remote_file_info=remote_file_info,
                previous_remote_file_info=previous_remote_metadata,
            )

            if not need.should_ingest:
                return False

            # --- 4. Download content ---
            ingest_result = self._download(version=version, remote_metadata=remote_file_info)

            # --- 5. Hash check (smart skip #2) but don't skip in healing mode ---
            if not need.is_healing and safe_previous_metadata:
                if self._should_skip_on_hash(
                    previous_hash=safe_previous_metadata.raw_file_sha256,
                    current_hash=ingest_result.download_info.file_hash,
                ):
                    return False

            return ingest_result
        except httpx.HTTPError as err:
            raise IngestStageError(self.dataset.name) from err

    def should_skip_extraction(
        self, extract_result: DownloadStageResult, previous_metadata: dict[str, Any] | None
    ) -> bool:
        """Check if extracted file hash matches previous run (smart skip).

        Parameters
        ----------
        extract_result:
            Download result with ``extraction_info`` populated.
        previous_metadata:
            Raw metadata dict from the previous run, or ``None``.

        Returns
        -------
        bool
            ``True`` if extraction output is identical and processing can be skipped.
        """
        safe_previous_metadata = self._safely_load_metadata(previous_metadata)
        if not safe_previous_metadata or not safe_previous_metadata.extracted_file_sha256:
            return False

        if extract_result.extraction_info is None:
            return False

        return self._should_skip_on_hash(
            safe_previous_metadata.extracted_file_sha256,
            extract_result.extraction_info.file_hash,
        )

    def extract_archive(self, ingest_result: DownloadStageResult) -> DownloadStageResult:
        """Extract target file from archive and populate ``extraction_info``.

        Parameters
        ----------
        ingest_result:
            Download result whose ``download_info.path`` points to the archive.

        Returns
        -------
        DownloadStageResult
            Copy of *ingest_result* with ``extraction_info`` populated.

        Raises
        ------
        ValueError
            If the dataset is not an archive or ``inner_file`` is missing.
        """
        try:
            if not self.dataset.source.format.is_archive or not self.dataset.source.inner_file:
                logger.warning(
                    "Trying to extract a non-archive format",
                    format=self.dataset.source.format.value,
                )
                raise ValueError(
                    f"Dataset {self.dataset.name} is not an archive or missing inner_file"
                )

            # Extract to same directory as archive (preserves directory structure)
            archive_path = ingest_result.download_info.path
            landing_dir = archive_path.parent

            extract_info = extract_7z(
                archive_path=archive_path,
                target_filename=self.dataset.source.inner_file,
                dest_dir=landing_dir,
                validate_sqlite=Path(self.dataset.source.inner_file).suffix == ".gpkg",
            )

            return DownloadStageResult(
                version=ingest_result.version,
                remote_metadata=ingest_result.remote_metadata,
                download_info=ingest_result.download_info,
                extraction_info=ExtractionInfo(
                    archive_path=archive_path,
                    file_path=extract_info.path,
                    file_hash=extract_info.file_hash,
                    size_mib=extract_info.size_mib,
                ),
            )
        except (
            ArchiveNotFoundError,
            FileNotFoundInArchiveError,
            FileIntegrityError,
            ValueError,
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
            logger.info("Content hash identical to previous run, cleaning up and skipping")
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

    def to_bronze(self, ingest_or_extract_result: DownloadStageResult) -> BronzeStageResult:
        """Convert landing file to versioned bronze Parquet.

        Steps: read landing → apply transform → write versioned parquet →
        update ``latest.parquet`` symlink → cleanup landing.

        Parameters
        ----------
        ingest_or_extract_result:
            Upstream download (or download + extraction) result.

        Returns
        -------
        BronzeStageResult
            Upstream download context and bronze metrics.
        """
        try:
            bronze_path = self.resolver.bronze_path(ingest_or_extract_result.version)
            bronze_path.parent.mkdir(parents=True, exist_ok=True)

            logger.info(
                "Converting to bronze",
                dataset_name=self.dataset.name,
                landing_path=ingest_or_extract_result.landing_path,
                bronze_path=bronze_path,
                version=ingest_or_extract_result.version,
            )

            # Retrieve and apply dataset-specific bronze transformation
            transform = get_bronze_transform(self.dataset.name)
            df = transform(ingest_or_extract_result.landing_path)
            df.write_parquet(bronze_path)

            # Update latest symlink to point to this version
            file_manager = RemoteFileManager(self.resolver)
            file_manager.update_bronze_latest_link(ingest_or_extract_result.version)

            # Cleanup all landing files
            self._cleanup_landing()

            columns = df.columns
            row_count = len(df)
            parquet_size = bronze_path.stat().st_size / (1024 * 1024)

            logger.info(
                "Bronze conversion complete",
                dataset_name=self.dataset.name,
                file_size_mib=parquet_size,
                row_count=row_count,
                columns=columns,
                bronze_path=bronze_path,
                latest_link=self.resolver.bronze_latest_path,
            )

            return BronzeStageResult(
                download=ingest_or_extract_result,
                bronze=BronzeInfo(
                    file_size_mib=parquet_size,
                    row_count=row_count,
                    columns=columns,
                ),
            )
        except (TransformNotFoundError, duckdb.Error, OSError) as err:
            raise BronzeStageError(self.dataset.name) from err

    def to_silver(self, bronze_result: BronzeStageResult) -> SilverStageResult:
        """Apply business transformations to create silver layer.

        Steps: rotate silver (current → backup) → read bronze latest →
        apply transform → write ``current.parquet``.

        Parameters
        ----------
        bronze_result:
            Upstream bronze conversion result.

        Returns
        -------
        SilverStageResult
            Upstream download/bronze context and silver metrics.
        """
        try:
            logger.info(
                "Transforming to silver",
                dataset_name=self.dataset.name,
                bronze_source=self.resolver.bronze_latest_path,
                silver_dest=self.resolver.silver_current_path,
            )

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
            parquet_size = self.resolver.silver_current_path.stat().st_size / (1024 * 1024)

            logger.info(
                "Silver transformation complete",
                dataset_name=self.dataset.name,
                file_size_mib=parquet_size,
                row_count=row_count,
                columns=columns,
                silver_current=self.resolver.silver_current_path,
                silver_backup=self.resolver.silver_backup_path,
            )

            return SilverStageResult(
                download=bronze_result.download,
                bronze=bronze_result.bronze,
                silver=SilverInfo(
                    file_size_mib=parquet_size,
                    row_count=row_count,
                    columns=columns,
                ),
            )
        except (TransformNotFoundError, duckdb.Error, OSError) as err:
            raise SilverStageError(self.dataset.name) from err

    @staticmethod
    def create_metadata_emission(silver_result: SilverStageResult) -> PipelineRunSnapshot:
        """Build a flattened snapshot from a completed silver pipeline run.

        Parameters
        ----------
        silver_result:
            Completed silver stage result containing all upstream context.

        Returns
        -------
        PipelineRunSnapshot
            Flat representation suitable for persistence in a metadata store.
        """
        dl = silver_result.download
        extraction = dl.extraction_info

        return PipelineRunSnapshot(
            # Download
            version=dl.version,
            remote_metadata=dl.remote_metadata,
            raw_file_sha256=dl.download_info.file_hash,
            raw_file_size_mib=dl.download_info.size_mib,
            # Extraction (optional)
            extracted_file_sha256=extraction.file_hash if extraction else None,
            extracted_file_size_mib=extraction.size_mib if extraction else None,
            # Bronze
            bronze_file_size_mib=silver_result.bronze.file_size_mib,
            bronze_row_count=silver_result.bronze.row_count,
            bronze_columns=silver_result.bronze.columns,
            # Silver
            silver_file_size_mib=silver_result.silver.file_size_mib,
            silver_row_count=silver_result.silver.row_count,
            silver_columns=silver_result.silver.columns,
        )
