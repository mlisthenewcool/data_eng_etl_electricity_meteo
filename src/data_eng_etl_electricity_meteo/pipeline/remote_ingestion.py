"""Pipeline manager for remote (HTTP) datasets.

Orchestrates the full ingestion pipeline for a single remote dataset, completely
decoupled from Airflow:

1. **Download** — HEAD check + smart-skip + download to landing
2. **Extract** — 7z extraction with SQLite header check (optional)
3. **Bronze** — landing → versioned Parquet + ``latest`` symlink
4. **Silver** — bronze latest → business transform → ``current`` / ``backup``

The download step can be customized via ``_custom_download``: a callable that takes a
landing directory and returns the path to the downloaded file.
When set, the standard HEAD check + single-URL download is replaced by custom logic
(e.g. multi-file merge for climatologie).
Smart-skip is still available for custom downloads via ``_custom_metadata``.

Uses ``RemotePathResolver`` for all path operations and ``RemoteFileManager`` for
symlinks, rotation, and rollback.
"""

import shutil
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import duckdb
import httpx
import polars as pl

from data_eng_etl_electricity_meteo.core.data_catalog import IngestionMode, RemoteDatasetConfig
from data_eng_etl_electricity_meteo.core.exceptions import (
    BronzeStageError,
    DownloadStageError,
    ExtractionError,
    ExtractStageError,
    SchemaValidationError,
    SilverStageError,
    SourceSchemaDriftError,
    TransformValidationError,
)
from data_eng_etl_electricity_meteo.core.logger import get_logger
from data_eng_etl_electricity_meteo.core.settings import settings
from data_eng_etl_electricity_meteo.pipeline.file_manager import RemoteFileManager
from data_eng_etl_electricity_meteo.pipeline.path_resolver import RemotePathResolver
from data_eng_etl_electricity_meteo.pipeline.progress import (
    AirflowBatchProgress,
    AirflowDownloadProgress,
    AirflowExtractProgress,
)
from data_eng_etl_electricity_meteo.pipeline.types import (
    BronzeMetrics,
    DownloadMetrics,
    ExtractionInfo,
    IncrementalDiffMetrics,
    IngestionDecision,
    PipelineContext,
    PipelineRunSnapshot,
    SilverMetrics,
)
from data_eng_etl_electricity_meteo.transformations.registry import get_transform_spec
from data_eng_etl_electricity_meteo.utils.download import (
    HttpDownloadInfo,
    download_to_file,
    shorten_url,
)
from data_eng_etl_electricity_meteo.utils.extraction import extract_7z
from data_eng_etl_electricity_meteo.utils.file_hash import FileHasher
from data_eng_etl_electricity_meteo.utils.polars import collect_narrow
from data_eng_etl_electricity_meteo.utils.progress import BatchProgressFactory
from data_eng_etl_electricity_meteo.utils.remote_metadata import (
    RemoteFileMetadata,
    get_remote_file_metadata,
)

logger = get_logger("pipeline")

# Custom download: takes (landing_dir, progress) and returns the produced file path.
# Replaces the standard single-URL download entirely (e.g. multi-file merge for
# climatologie). The implementation manages its own HTTP client and timeouts.
CustomDownloadFunc = Callable[[Path, BatchProgressFactory | None], Path]

# Custom metadata: takes a URL and returns remote file metadata.
# Fallback when HTTP HEAD returns no caching headers (e.g. OpenDataSoft).
# The implementation manages its own HTTP client and timeouts.
CustomMetadataFunc = Callable[[str], RemoteFileMetadata]


@dataclass(frozen=True)
class RemoteIngestionPipeline:
    """Pipeline manager for a single remote dataset.

    Orchestrates: download → (extract) → to_bronze → to_silver.

    Attributes
    ----------
    dataset
        Remote dataset configuration from the catalog.
    _custom_download
        Optional download strategy. When set, replaces the standard HEAD check +
        single-URL download with custom logic. The callable receives the landing
        directory and must return the path to the produced file.
    _custom_metadata
        Optional metadata strategy. For standard downloads, called as a fallback when
        the standard HTTP HEAD returns no caching headers. For custom downloads, called
        before the download to enable metadata-based smart-skip.

    Raises
    ------
    TransformNotFoundError
        At construction time if the transform spec is not registered.
    """

    dataset: RemoteDatasetConfig
    _custom_download: CustomDownloadFunc | None = field(default=None, repr=False)
    _custom_metadata: CustomMetadataFunc | None = field(default=None, repr=False)
    _resolver: RemotePathResolver = field(init=False, repr=False)
    _file_manager: RemoteFileManager = field(init=False, repr=False)

    def __post_init__(self) -> None:
        """Validate transform spec and initialize derived fields."""
        get_transform_spec(self.dataset.name)
        object.__setattr__(
            self,
            "_resolver",
            RemotePathResolver(dataset_name=self.dataset.name),
        )
        object.__setattr__(
            self,
            "_file_manager",
            RemoteFileManager(self._resolver),
        )

    # -- Download ----------------------------------------------------------------------

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
            Freshly fetched remote metadata.
        previous_remote_file_info
            Metadata from the previous successful run, or ``None``.

        Returns
        -------
        IngestionDecision
            Whether to ingest, whether this is a healing run, and the remote metadata
            used for the decision.
        """
        silver_file_exists = self._resolver.silver_current_path.exists()
        # Treat empty metadata (all None fields from stale state) as absent.
        has_previous_metadata = (
            previous_remote_file_info is not None and previous_remote_file_info.has_any_field()
        )

        if has_previous_metadata != silver_file_exists:
            logger.warning(
                "Inconsistent state, forcing ingestion to heal",
                silver_file_exists=silver_file_exists,
                has_previous_metadata=has_previous_metadata,
            )
            return IngestionDecision(
                should_ingest=True, is_healing=True, remote_metadata=current_remote_file_info
            )

        # After the guard above, has_previous_metadata == silver_file_exists.
        # Redundant is-not-None narrowing required by ty (can't follow boolean guard).
        if has_previous_metadata and previous_remote_file_info is not None:
            change_detection_result = current_remote_file_info.compare_with(
                previous_remote_file_info
            )
            if not change_detection_result.has_changed:
                logger.info(
                    "Ingestion skipped: remote metadata unchanged",
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

        For custom downloads with a registered ``_custom_metadata``, performs a
        metadata-based smart-skip check before downloading, then a content hash
        comparison after.
        Without ``_custom_metadata``, delegates directly to the custom callable.
        For standard downloads, performs HEAD check + smart-skip + single-URL download.

        Returns ``None`` if ingestion was skipped (unchanged remote or identical
        content hash), otherwise the initial pipeline context.

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
            or the download, I/O error writing the file, or if the custom download
            fails.
        """
        if self._custom_download is not None:
            return self._run_custom_download(version, previous_snapshot)

        return self._run_standard_download(version, previous_snapshot)

    def _run_custom_download(
        self, version: str, previous_snapshot: PipelineRunSnapshot | None
    ) -> PipelineContext | None:
        """Execute the custom download strategy with smart-skip support.

        When ``_custom_metadata`` is registered, performs two smart-skip layers before
        and after the download:

        1. **Pre-download** — compare remote metadata with previous snapshot
           (e.g. dataset-level ``last_update`` from data.gouv.fr).
        2. **Post-download** — compare content hash with previous snapshot.

        Parameters
        ----------
        version
            Run version string.
        previous_snapshot
            Validated snapshot from the previous run, or ``None``.

        Returns
        -------
        PipelineContext | None
            Pipeline context with the landing file path, or ``None`` if skipped.

        Raises
        ------
        DownloadStageError
            If the custom download callable fails.
        """
        assert self._custom_download is not None  # type narrowing: guarded by download() dispatch

        t0 = time.monotonic()
        logger.info("Starting custom download", version=version)

        # -- 1. Smart-skip: remote metadata comparison ---------------------

        remote_metadata = RemoteFileMetadata()
        ingestion_decision: IngestionDecision | None = None

        if self._custom_metadata is not None:
            try:
                remote_metadata = self._custom_metadata(self.dataset.source.url_as_str)
            except (httpx.HTTPError, ValueError) as err:
                logger.warning(
                    "Custom metadata fetch failed, proceeding with download",
                    error=str(err),
                )

            if previous_snapshot is not None and remote_metadata.has_any_field():
                ingestion_decision = self._decide_ingestion(
                    remote_metadata,
                    previous_snapshot.download.remote_metadata,
                )
                if not ingestion_decision.should_ingest:
                    return None

        # -- 2. Custom download --------------------------------------------

        progress: BatchProgressFactory | None = (
            AirflowBatchProgress if settings.is_running_on_airflow else None
        )

        try:
            landing_path = self._custom_download(self._resolver.landing_dir, progress)
        except (httpx.HTTPError, OSError, ValueError) as err:
            raise DownloadStageError("Custom download failed") from err

        try:
            file_hash = FileHasher.hash_file(landing_path)
            size_mib = round(landing_path.stat().st_size / (1024 * 1024), 2)
        except OSError as err:
            raise DownloadStageError("Failed to hash downloaded file") from err

        logger.info(
            "Custom download completed",
            file_size_mib=size_mib,
            duration_s=round(time.monotonic() - t0, 2),
        )

        # -- 3. Build context and smart-skip on content hash ---------------

        context = PipelineContext(
            version=version,
            is_healing=ingestion_decision.is_healing if ingestion_decision else False,
            download=DownloadMetrics(
                remote_metadata=remote_metadata,
                download_info=HttpDownloadInfo(
                    path=landing_path,
                    file_hash=file_hash,
                    size_mib=size_mib,
                ),
            ),
        )

        if not context.is_healing and previous_snapshot is not None:
            if self._should_skip_on_hash(
                previous_hash=previous_snapshot.download.file_hash,
                current_hash=file_hash,
            ):
                self._cleanup_landing()
                return None

        return context

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
            On any HTTP or I/O failure during metadata fetch or file download.
        """
        t0 = time.monotonic()
        logger.info(
            "Starting download",
            version=version,
            url=shorten_url(self.dataset.source.url_as_str),
        )

        previous_remote_metadata = (
            previous_snapshot.download.remote_metadata if previous_snapshot else None
        )
        previous_etag = previous_remote_metadata.etag if previous_remote_metadata else None

        # -- 1. Fetch current remote metadata (HEAD request) ---------------------------
        # Send If-None-Match when we have a previous ETag so the server can
        # short-circuit with 304 Not Modified.

        try:
            remote_file_info = get_remote_file_metadata(
                self.dataset.source.url_as_str,
                if_none_match=previous_etag,
            )
        except httpx.HTTPError as err:
            raise DownloadStageError("HEAD metadata fetch failed") from err

        # -- 1b. Custom metadata fallback (e.g. OpenDataSoft catalog API) --------------
        # When HEAD returns no caching headers and a custom strategy is registered,
        # call it to obtain metadata from an alternative source.
        # Hard failure here (unlike custom download path) because for these datasets
        # HEAD is useless — the custom metadata is the only source of change detection.

        if remote_file_info is not None and not remote_file_info.has_any_field():
            if self._custom_metadata is not None:
                try:
                    remote_file_info = self._custom_metadata(self.dataset.source.url_as_str)
                except (httpx.HTTPError, ValueError) as err:
                    raise DownloadStageError("Custom metadata fetch failed") from err

        # -- 2. Smart-skip #1: HTTP 304 or metadata comparison -------------------------

        if remote_file_info is None:
            # Server confirmed no change (304). Still need to check local state.
            if self._resolver.silver_current_path.exists():
                logger.info(
                    "Ingestion skipped: remote metadata unchanged",
                    reason="HTTP 304 Not Modified",
                )
                return None

            # Silver file missing despite 304 — healing needed: re-fetch full metadata.
            logger.warning("HTTP 304 but silver file missing, re-fetching metadata to heal")
            try:
                remote_file_info = get_remote_file_metadata(
                    self.dataset.source.url_as_str,
                )
            except httpx.HTTPError as err:
                raise DownloadStageError("HEAD metadata re-fetch failed") from err
            assert remote_file_info is not None  # no If-None-Match → never 304

        ingestion_decision = self._decide_ingestion(remote_file_info, previous_remote_metadata)

        if not ingestion_decision.should_ingest:
            return None

        # -- 3. Download ---------------------------------------------------------------

        try:
            download_info = download_to_file(
                self.dataset.source.url_as_str,
                dest_dir=self._resolver.landing_dir,
                fallback_filename=f"{version}.{self.dataset.source.format.value}",
                timeout_seconds=settings.download_timeout_seconds,
                progress=AirflowDownloadProgress if settings.is_running_on_airflow else None,
            )
        except (httpx.HTTPError, OSError) as err:
            raise DownloadStageError(
                f"File download failed: {self.dataset.source.url_as_str}"
            ) from err

        context = PipelineContext(
            version=version,
            is_healing=ingestion_decision.is_healing,
            download=DownloadMetrics(
                remote_metadata=remote_file_info,
                download_info=download_info,
            ),
        )

        logger.info(
            "Download completed",
            file_size_mib=download_info.size_mib,
            duration_s=round(time.monotonic() - t0, 2),
        )

        # -- 4. Smart-skip #2: content hash comparison (skipped in healing mode) -------

        if not context.is_healing and previous_snapshot is not None:
            if self._should_skip_on_hash(
                previous_hash=previous_snapshot.download.file_hash,
                current_hash=context.download.download_info.file_hash,
            ):
                self._cleanup_landing()
                return None

        return context

    # -- Extraction --------------------------------------------------------------------

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
        t0 = time.monotonic()
        logger.info("Starting extraction")
        archive_path = context.download.download_info.path
        landing_dir = archive_path.parent

        # inner_file is guaranteed non-None by RemoteSourceConfig's validator for
        # archive formats, but ty cannot prove it here. The check guards against
        # any future regression and also narrows the type for the call below.
        inner_file = self.dataset.source.inner_file
        if inner_file is None:
            raise ExtractStageError("inner_file required for archive dataset")

        try:
            extract_info = extract_7z(
                archive_path,
                target_filename=inner_file,
                dest_dir=landing_dir,
                validate_sqlite=Path(inner_file).suffix == ".gpkg",
                progress=AirflowExtractProgress if settings.is_running_on_airflow else None,
            )
        except ExtractionError as err:
            raise ExtractStageError("Archive extraction failed") from err

        logger.info(
            "Extraction completed",
            file_size_mib=extract_info.size_mib,
            duration_s=round(time.monotonic() - t0, 2),
        )

        # -- Build result context ------------------------------------------------------

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

        # -- Smart-skip: hash comparison against previous extraction -------------------

        if not context.is_healing and previous_snapshot and previous_snapshot.extraction:
            if self._should_skip_on_hash(
                previous_hash=previous_snapshot.extraction.file_hash,
                current_hash=extract_info.file_hash,
            ):
                self._cleanup_landing()
                return None

        return updated_context

    # -- Helpers -----------------------------------------------------------------------

    @staticmethod
    def _should_skip_on_hash(*, previous_hash: str | None, current_hash: str) -> bool:
        """Return ``True`` if hashes match (content unchanged since last run)."""
        if previous_hash == current_hash:
            logger.info(
                "Ingestion skipped: content hash identical to previous run",
            )
            return True
        return False

    def _cleanup_landing(self) -> None:
        """Remove the landing directory and all its contents."""
        if self._resolver.landing_dir.exists() and self._resolver.landing_dir.is_dir():
            shutil.rmtree(self._resolver.landing_dir)

    # -- Transformations ---------------------------------------------------------------

    def to_bronze(self, context: PipelineContext) -> PipelineContext:
        """Convert landing file to versioned bronze Parquet.

        Steps: read landing → apply transform → write versioned Parquet → update
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
            On transform failure reading the landing file (DuckDB / Polars / I/O),
            Parquet write failure, or symlink update failure.
        """
        bronze_path = self._resolver.bronze_path(context.version)
        bronze_path.parent.mkdir(parents=True, exist_ok=True)

        t0 = time.monotonic()
        logger.info("Starting bronze conversion")

        # TransformNotFoundError propagates directly
        # (programming error, fast-fail by __post_init__)

        # -- Apply transform and write parquet -----------------------------------------

        spec = get_transform_spec(self.dataset.name)

        try:
            lf = spec.bronze_transform(context.download.landing_path)
        except (duckdb.Error, pl.exceptions.PolarsError, OSError) as err:
            raise BronzeStageError("Bronze transform failed (reading landing file)") from err

        try:
            lf.sink_parquet(bronze_path)
        except (pl.exceptions.PolarsError, OSError) as err:
            raise BronzeStageError("Bronze Parquet write failed") from err

        # -- Update symlink and cleanup landing ----------------------------------------

        try:
            self._file_manager.update_bronze_latest_link(context.version)
        except OSError as err:
            raise BronzeStageError("Bronze symlink update failed") from err

        self._cleanup_landing()

        # -- Collect output metadata without loading all rows --------------------------

        try:
            bronze_lf = pl.scan_parquet(bronze_path)
            columns = list(bronze_lf.collect_schema().names())
            rows_count: int = collect_narrow(bronze_lf.select(pl.len())).item(0, 0)
            parquet_size = round(bronze_path.stat().st_size / (1024 * 1024), 2)
        except (pl.exceptions.PolarsError, OSError) as err:
            raise BronzeStageError("Failed to read bronze metadata after write") from err

        logger.info(
            "Bronze conversion completed",
            rows_count=rows_count,
            columns_count=len(columns),
            file_size_mib=parquet_size,
            duration_s=round(time.monotonic() - t0, 2),
        )

        return PipelineContext(
            version=context.version,
            download=context.download,
            bronze=BronzeMetrics(
                file_size_mib=parquet_size,
                rows_count=rows_count,
                columns=columns,
            ),
        )

    def to_silver(self, context: PipelineContext) -> PipelineContext:
        """Apply business transformations to create silver layer.

        For incremental datasets, computes a delta (new + changed rows) by comparing the
        new transform output against the previous ``current.parquet``. The delta is
        written to ``delta.parquet`` alongside the full snapshot in ``current.parquet``.

        The diff is computed **after** writing the new snapshot and freeing the
        DataFrame from memory. Both Parquet files (current and backup) are then scanned
        lazily so that the full datasets are never loaded simultaneously. This keeps
        peak memory at ~2 GB (one DataFrame) instead of ~8 GB
        (two DataFrames + a full-column join), which caused OOM kills on the 18M-row
        climatologie dataset when other DAGs ran in parallel.

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
            On source schema drift (columns added/removed), silver output validation
            failure, transform/read failure (DuckDB / Polars / I/O), file rotation
            failure, Parquet write failure, or delta write failure.
        """
        t0 = time.monotonic()
        logger.info("Starting silver transformation")

        # TransformNotFoundError propagates directly
        # (programming error, fast-fail by __post_init__)

        # -- Execute silver transform --------------------------------------------------

        spec = get_transform_spec(dataset_name=self.dataset.name)

        try:
            df = spec.run_silver(self._resolver.bronze_latest_path)
        except SourceSchemaDriftError as err:
            raise SilverStageError(
                "Source API schema has changed (columns added or removed)"
            ) from err
        except (SchemaValidationError, TransformValidationError) as err:
            raise SilverStageError("Silver output validation failed") from err
        except (duckdb.Error, pl.exceptions.PolarsError, OSError) as err:
            raise SilverStageError("Silver transform or read failed") from err

        # -- Rotate files and write new snapshot ---------------------------------------

        is_incremental = self.dataset.ingestion.mode == IngestionMode.INCREMENTAL
        diff_metrics: IncrementalDiffMetrics | None = None

        # Rotate silver: current → backup, write new current, then free df.
        # The diff is computed AFTER freeing df, lazily from the two Parquet
        # files (current vs backup), to avoid holding two full DataFrames
        # in memory simultaneously (~4 GB for 18M-row datasets → OOM).
        try:
            self._file_manager.rotate_silver()
        except OSError as err:
            raise SilverStageError("Silver file rotation failed (current → backup)") from err

        try:
            self._resolver.silver_current_path.parent.mkdir(parents=True, exist_ok=True)
            df.write_parquet(self._resolver.silver_current_path)
        except OSError as err:
            self._file_manager.rollback_silver()
            raise SilverStageError("Silver Parquet write failed") from err

        columns = df.columns
        rows_count = len(df)
        del df  # free ~2 GB before diff computation

        # -- Compute incremental diff if applicable ------------------------------------

        if is_incremental:
            try:
                diff_metrics, delta = self._compute_incremental_diff_from_files()
            except (pl.exceptions.PolarsError, OSError) as err:
                raise SilverStageError("Silver incremental diff computation failed") from err
            try:
                delta.write_parquet(self._resolver.silver_delta_path)
            except OSError as err:
                raise SilverStageError("Silver delta write failed") from err

        try:
            size_bytes = self._resolver.silver_current_path.stat().st_size
            parquet_size = round(size_bytes / (1024 * 1024), 2)
        except OSError as err:
            raise SilverStageError("Failed to stat silver Parquet after write") from err

        # -- Build metrics and result --------------------------------------------------

        log_kwargs: dict[str, object] = {
            "rows_count": rows_count,
            "columns_count": len(columns),
            "file_size_mib": parquet_size,
        }
        if diff_metrics:
            log_kwargs.update(
                rows_added=diff_metrics.rows_added,
                rows_changed=diff_metrics.rows_changed,
                rows_unchanged=diff_metrics.rows_unchanged,
            )

        log_kwargs["duration_s"] = round(time.monotonic() - t0, 2)
        logger.info("Silver transformation completed", **log_kwargs)

        return PipelineContext(
            version=context.version,
            download=context.download,
            bronze=context.bronze,
            silver=SilverMetrics(
                file_size_mib=parquet_size,
                rows_count=rows_count,
                columns=columns,
                diff=diff_metrics,
            ),
        )

    def _compute_incremental_diff_from_files(
        self,
    ) -> tuple[IncrementalDiffMetrics, pl.DataFrame]:
        """Compute the delta between current (new) and backup (old) Parquet files.

        Both files are scanned lazily so that the full datasets are never loaded into
        memory simultaneously. Only the delta rows (new + changed) are collected —
        typically a tiny fraction of the total.

        Must be called **after** ``rotate_silver`` + ``write_parquet`` so that
        ``silver_current_path`` = new data and ``silver_backup_path`` = old data.

        Returns
        -------
        tuple[IncrementalDiffMetrics, pl.DataFrame]
            Diff statistics and the delta DataFrame (new + changed rows only).
        """
        pk = self.dataset.primary_key
        new_lf = pl.scan_parquet(self._resolver.silver_current_path)

        # -- Handle first run (no previous snapshot) -----------------------------------

        if not self._resolver.silver_backup_path.exists():
            # First run: everything is new
            new_df = collect_narrow(new_lf)
            logger.info("No previous silver snapshot, full delta", rows_count=len(new_df))
            return (
                IncrementalDiffMetrics(
                    rows_total=len(new_df),
                    rows_added=len(new_df),
                    rows_changed=0,
                    rows_unchanged=0,
                ),
                new_df,
            )

        # -- Load and align previous snapshot ------------------------------------------

        old_lf = pl.scan_parquet(self._resolver.silver_backup_path)

        # Align types: previous may have different dtypes due to schema evolution
        old_lf = old_lf.cast(new_lf.collect_schema())

        # -- New rows: anti-join on PK (old file only loads PK columns) ----------------

        df_new = collect_narrow(new_lf.join(old_lf.select(pk), on=pk, how="anti"))

        # -- Changed rows: hash-based comparison instead of full column join -----------

        # pl.struct hashing is null-aware, so null vs value diffs are detected.
        non_key_cols = [c for c in new_lf.collect_schema().names() if c not in pk]
        hash_expr = pl.struct(non_key_cols).hash().alias("_row_hash")

        # Collect inner join (PK + row hashes) in one pass to derive both
        # changed-row PKs and rows_in_both (avoids an extra full-file scan
        # just for rows_total).
        matched_lf = new_lf.select([*pk, hash_expr]).join(
            old_lf.select([*pk, hash_expr]),
            on=pk,
            how="inner",
            suffix="_prev",
        )
        matched_df = collect_narrow(matched_lf)

        rows_in_both = len(matched_df)
        changed_pks = matched_df.filter(pl.col("_row_hash") != pl.col("_row_hash_prev")).select(pk)
        del matched_df  # free PK+hash frame before collecting full rows

        df_changed = collect_narrow(new_lf.join(changed_pks.lazy(), on=pk, how="inner"))

        # -- Build delta and compute metrics -------------------------------------------

        delta = pl.concat([df_new, df_changed])
        rows_total = len(df_new) + rows_in_both
        rows_unchanged = rows_in_both - len(df_changed)

        metrics = IncrementalDiffMetrics(
            rows_total=rows_total,
            rows_added=len(df_new),
            rows_changed=len(df_changed),
            rows_unchanged=rows_unchanged,
        )

        logger.debug(
            "Incremental diff computed",
            rows_count=metrics.rows_total,
            rows_added=metrics.rows_added,
            rows_changed=metrics.rows_changed,
            rows_unchanged=metrics.rows_unchanged,
            rows_delta=len(delta),
        )

        return metrics, delta
