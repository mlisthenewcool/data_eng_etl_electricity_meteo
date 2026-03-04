"""Benchmark silver-layer loading into Postgres.

Measures actual throughput (rows/sec) for each silver dataset using the production
``load_silver_to_postgres`` function.
Results are logged via structlog and optionally written to a JSON file.

Requirements
    - Postgres running (``docker compose up -d postgres_service``)
    - Silver parquet files exist (run the pipeline first)

Usage::

    PYTHONPATH=src uv run python scripts/benchmarks/bench_postgres_load.py
    PYTHONPATH=src uv run python scripts/benchmarks/bench_postgres_load.py \
        --json results.json
    PYTHONPATH=src uv run python scripts/benchmarks/bench_postgres_load.py \
        --dataset odre_installations
"""

import json
import time
from pathlib import Path
from typing import Any

import polars as pl
import typer

from data_eng_etl_electricity_meteo.core.data_catalog import DataCatalog, RemoteDatasetConfig
from data_eng_etl_electricity_meteo.core.logger import get_logger
from data_eng_etl_electricity_meteo.core.settings import settings
from data_eng_etl_electricity_meteo.loaders.pg_connection import open_standalone_connection
from data_eng_etl_electricity_meteo.loaders.pg_loader import load_silver_to_postgres
from data_eng_etl_electricity_meteo.pipeline.path_resolver import RemotePathResolver

logger = get_logger("benchmark.postgres_load")

app = typer.Typer(add_completion=False)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parquet_stats(path: Path) -> dict[str, Any]:
    """Read row count and file size from a Parquet file without loading data."""
    if not path.exists():
        return {"exists": False, "rows": 0, "size_mb": 0.0}
    count_df = pl.scan_parquet(path).select(pl.len()).collect()
    assert isinstance(count_df, pl.DataFrame)
    count = count_df.item()
    size_mb = round(path.stat().st_size / (1024 * 1024), 2)
    return {"exists": True, "rows": count, "size_mb": size_mb}


def _benchmark_one(
    dataset: RemoteDatasetConfig,
    conn: Any,
) -> dict[str, Any]:
    """Load a single dataset and return timing metrics."""
    resolver = RemotePathResolver(dataset.name)
    parquet_path = resolver.silver_current_path
    stats = _parquet_stats(parquet_path)

    if not stats["exists"]:
        logger.warning("Silver parquet not found, skipping", dataset=dataset.name)
        return {
            "dataset": dataset.name,
            "success": False,
            "error": f"File not found: {parquet_path}",
        }

    mode = dataset.ingestion.mode.value
    logger.info(
        "Starting load",
        dataset=dataset.name,
        mode=mode,
        rows=stats["rows"],
        size_mb=stats["size_mb"],
    )

    start = time.perf_counter()
    try:
        metrics = load_silver_to_postgres(dataset, conn)
        elapsed = time.perf_counter() - start
        rows_per_sec = round(stats["rows"] / elapsed) if elapsed > 0 else 0

        logger.info(
            "Load completed",
            dataset=dataset.name,
            elapsed_sec=round(elapsed, 2),
            rows=metrics.rows_loaded,
            rows_per_sec=rows_per_sec,
        )

        return {
            "dataset": dataset.name,
            "success": True,
            "mode": mode,
            "file_rows": stats["rows"],
            "file_size_mb": stats["size_mb"],
            "rows_loaded": metrics.rows_loaded,
            "elapsed_sec": round(elapsed, 2),
            "rows_per_sec": rows_per_sec,
        }
    except Exception as exc:
        elapsed = time.perf_counter() - start
        logger.error("Load failed", dataset=dataset.name, error=str(exc))
        return {
            "dataset": dataset.name,
            "success": False,
            "error": str(exc),
            "elapsed_sec": round(elapsed, 2),
        }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@app.command()
def main(
    dataset: list[str] | None = typer.Option(
        None,
        "--dataset",
        "-d",
        help="Run only for these datasets (repeatable). Omit to run all.",
    ),
    json_output: Path | None = typer.Option(
        None,
        "--json",
        help="Write results to a JSON file.",
    ),
) -> None:
    """Benchmark silver-layer Postgres loading for all (or selected) datasets."""
    catalog = DataCatalog.load(settings.data_catalog_file_path)
    remote_datasets = catalog.get_remote_datasets()

    if dataset:
        names = set(dataset)
        remote_datasets = [d for d in remote_datasets if d.name in names]
        missing = names - {d.name for d in remote_datasets}
        if missing:
            logger.warning("Datasets not found in catalog", missing=sorted(missing))

    if not remote_datasets:
        logger.error("No datasets to benchmark")
        raise typer.Exit(1)

    conn = open_standalone_connection()
    results: list[dict[str, Any]] = []

    try:
        for ds in remote_datasets:
            result = _benchmark_one(ds, conn)
            results.append(result)
    finally:
        conn.close()

    # --- Summary ---
    succeeded = [r for r in results if r.get("success")]
    total_rows = sum(r.get("file_rows", 0) for r in succeeded)
    total_time = sum(r.get("elapsed_sec", 0) for r in succeeded)

    logger.info(
        "Benchmark complete",
        datasets_run=len(results),
        datasets_ok=len(succeeded),
        total_rows=total_rows,
        total_time_sec=round(total_time, 2),
        avg_rows_per_sec=round(total_rows / total_time) if total_time > 0 else 0,
    )

    if json_output:
        json_output.write_text(json.dumps(results, indent=2, ensure_ascii=False))
        logger.info("Results written", path=str(json_output))


if __name__ == "__main__":
    app()
