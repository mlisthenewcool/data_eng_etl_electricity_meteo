"""Inventory and benchmark framework for tunable performance parameters.

Lists every configurable or hardcoded parameter that affects pipeline throughput, with
its current value, source location, and suggested test range.
Individual benchmark functions are stubs — implement them as needed.

Usage::

    PYTHONPATH=src uv run python scripts/benchmarks/bench_tunable_parameters.py list
    PYTHONPATH=src uv run python scripts/benchmarks/bench_tunable_parameters.py \
        run download_chunk_size
    PYTHONPATH=src uv run python scripts/benchmarks/bench_tunable_parameters.py \
        run all
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import typer

from data_eng_etl_electricity_meteo.core.logger import get_logger

logger = get_logger("benchmark.tunable")

app = typer.Typer(add_completion=False)


# ---------------------------------------------------------------------------
# Parameter registry
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TunableParameter:
    """A performance-sensitive parameter that can be benchmarked."""

    name: str
    current_value: str
    source_file: str
    source_line: int
    priority: int  # 1 = high impact, 2 = medium, 3 = low
    test_values: list[Any] = field(default_factory=list)
    description: str = ""
    implemented: bool = False


PARAMETERS: list[TunableParameter] = [
    # --- Priority 1: High impact on throughput ---
    TunableParameter(
        name="download_chunk_size",
        current_value="512 KB",
        source_file="core/settings.py",
        source_line=170,
        priority=1,
        test_values=["256 KB", "512 KB", "1 MB", "2 MB", "5 MB"],
        description=(
            "httpx streaming read size. Larger chunks reduce syscall overhead but"
            " increase memory per concurrent download. Affects climatologie (95"
            " parallel department files) the most."
        ),
    ),
    TunableParameter(
        name="copy_buffer_size",
        current_value="64 KB",
        source_file="loaders/pg_loader.py",
        source_line=44,
        priority=1,
        test_values=["16 KB", "32 KB", "64 KB", "128 KB", "256 KB"],
        description=(
            "psycopg COPY streaming I/O chunk size. Controls how much CSV data is"
            " flushed to Postgres per write() call. Too small = syscall overhead,"
            " too large = memory waste on small datasets."
        ),
    ),
    TunableParameter(
        name="copy_spool_threshold",
        current_value="128 MB",
        source_file="loaders/pg_loader.py",
        source_line=45,
        priority=1,
        test_values=["64 MB", "128 MB", "256 MB", "512 MB"],
        description=(
            "SpooledTemporaryFile threshold. CSV data is buffered in RAM below this"
            " limit, spilled to disk above. Critical for the 18M-row climatologie"
            " dataset (~760 MB CSV). Too low = unnecessary disk I/O, too high = OOM"
            " risk when other DAGs run in parallel."
        ),
    ),
    TunableParameter(
        name="hash_chunk_size",
        current_value="128 KB",
        source_file="core/settings.py",
        source_line=217,
        priority=1,
        test_values=["64 KB", "128 KB", "256 KB", "512 KB", "1 MB"],
        description=(
            "File hashing read buffer. Used for integrity checks on landing files."
            " I/O-bound — larger chunks reduce read() calls but have diminishing"
            " returns above ~256 KB on most disks."
        ),
    ),
    # --- Priority 2: Network and timeouts ---
    TunableParameter(
        name="download_timeout_total",
        current_value="600s",
        source_file="core/settings.py",
        source_line=177,
        priority=2,
        test_values=["300s", "600s", "900s"],
        description=(
            "Total httpx timeout for a single file download. Must cover the slowest"
            " dataset (eco2mix_cons_def ~260 MB). Too tight = intermittent failures"
            " on slow connections."
        ),
    ),
    TunableParameter(
        name="download_timeout_connect",
        current_value="10s",
        source_file="core/settings.py",
        source_line=184,
        priority=2,
        test_values=["5s", "10s", "20s"],
        description=(
            "TCP connection timeout. Affects how fast a down server is detected."
            " data.gouv.fr occasionally has slow DNS resolution."
        ),
    ),
    TunableParameter(
        name="download_timeout_sock_read",
        current_value="30s",
        source_file="core/settings.py",
        source_line=191,
        priority=2,
        test_values=["15s", "30s", "60s"],
        description=(
            "Socket read timeout between chunks. A stall longer than this aborts"
            " the download. Météo France API can be bursty."
        ),
    ),
    # --- Priority 3: Algorithmic choices ---
    TunableParameter(
        name="hash_algorithm",
        current_value="sha256",
        source_file="core/settings.py",
        source_line=212,
        priority=3,
        test_values=["md5", "sha1", "sha256", "sha512"],
        description=(
            "Hashing algorithm for file integrity checks. sha256 is the safe"
            " default. md5/sha1 are faster but cryptographically broken (fine for"
            " integrity, not security). xxhash would be fastest but requires an"
            " extra dependency."
        ),
    ),
    TunableParameter(
        name="incremental_diff_strategy",
        current_value="hash (pl.struct.hash)",
        source_file="pipeline/remote_ingestion.py",
        source_line=640,
        priority=3,
        test_values=["hash_struct", "full_column_join"],
        description=(
            "How changed rows are detected between two silver snapshots."
            " Hash-based is memory-efficient but misses hash collisions (extremely"
            " rare). Full-column join is exact but uses ~4x more memory."
        ),
    ),
    TunableParameter(
        name="polars_streaming_engine",
        current_value="sink_parquet (streaming)",
        source_file="pipeline/remote_ingestion.py",
        source_line=463,
        priority=3,
        test_values=["collect+write_parquet", "sink_parquet"],
        description=(
            "Bronze transform output strategy. sink_parquet streams in chunks"
            " (lower peak memory). collect+write_parquet loads everything first"
            " (faster for small datasets, OOM risk for large ones)."
        ),
    ),
]


# ---------------------------------------------------------------------------
# Benchmark stubs
# ---------------------------------------------------------------------------


def _bench_download_chunk_size() -> None:
    """Benchmark varying download_chunk_size on a real file download."""
    raise NotImplementedError(
        "Download a medium-sized file (e.g. eco2mix_tr ~30 MB) with each chunk"
        " size and measure elapsed time + memory. Repeat 3x for variance."
    )


def _bench_copy_buffer_size() -> None:
    """Benchmark varying _COPY_BUFFER_SIZE on Postgres COPY."""
    raise NotImplementedError(
        "Load odre_installations (~124k rows) with each buffer size."
        " Measure elapsed time. Requires making _COPY_BUFFER_SIZE a parameter"
        " of _copy_df() instead of a module constant."
    )


def _bench_copy_spool_threshold() -> None:
    """Benchmark varying _COPY_SPOOL_THRESHOLD on large datasets."""
    raise NotImplementedError(
        "Load meteo_france_climatologie (~18M rows) with each threshold."
        " Monitor peak RSS (resource.getrusage) and elapsed time."
        " Test with and without parallel load to simulate Airflow."
    )


def _bench_hash_chunk_size() -> None:
    """Benchmark varying hash_chunk_size on a large landing file."""
    raise NotImplementedError(
        "Hash ADMIN-EXPRESS-COG.7z (~1 GB) or the climatologie merged parquet"
        " with each chunk size. Measure MB/s throughput."
    )


def _bench_hash_algorithm() -> None:
    """Compare hash algorithm speed on the same file."""
    raise NotImplementedError(
        "Hash a ~500 MB file with each algorithm. Report MB/s."
        " Consider adding xxhash to the comparison."
    )


def _bench_incremental_diff_strategy() -> None:
    """Compare hash-based vs full-column-join diff on climatologie."""
    raise NotImplementedError(
        "Run both strategies on climatologie current vs backup."
        " Measure elapsed time AND peak memory (tracemalloc)."
    )


_BENCH_REGISTRY: dict[str, Callable[[], None]] = {
    "download_chunk_size": _bench_download_chunk_size,
    "copy_buffer_size": _bench_copy_buffer_size,
    "copy_spool_threshold": _bench_copy_spool_threshold,
    "hash_chunk_size": _bench_hash_chunk_size,
    "hash_algorithm": _bench_hash_algorithm,
    "incremental_diff_strategy": _bench_incremental_diff_strategy,
}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

PRIORITY_LABELS: dict[int, str] = {
    1: "High impact",
    2: "Network / timeouts",
    3: "Algorithmic",
}


@app.command("list")
def list_params() -> None:
    """Display all tunable parameters with their current values."""
    for priority in (1, 2, 3):
        params = [p for p in PARAMETERS if p.priority == priority]
        if not params:
            continue

        logger.info(f"Priority {priority}: {PRIORITY_LABELS[priority]}")

        for p in params:
            logger.info(
                p.name,
                current=p.current_value,
                source=f"{p.source_file}:{p.source_line}",
                test_values=p.test_values,
                implemented=p.implemented,
            )


@app.command("run")
def run_bench(
    name: str = typer.Argument(help="Benchmark name (or 'all')."),
) -> None:
    """Run a specific benchmark (or all implemented ones)."""
    if name == "all":
        targets = list(_BENCH_REGISTRY.items())
    elif name in _BENCH_REGISTRY:
        targets = [(name, _BENCH_REGISTRY[name])]
    else:
        logger.error("Unknown benchmark", name=name, available=sorted(_BENCH_REGISTRY))
        raise typer.Exit(1)

    for bench_name, bench_fn in targets:
        logger.info("Running benchmark", name=bench_name)
        try:
            bench_fn()
            logger.info("Benchmark completed", name=bench_name)
        except NotImplementedError as exc:
            logger.warning("Not yet implemented", name=bench_name, hint=str(exc))


if __name__ == "__main__":
    app()
