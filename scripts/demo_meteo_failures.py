"""Run download_climatologie with simulated department failures.

Demonstrates error handling, progress bar, and logging behavior with real HTTP downloads
for a reduced set of departments.

Downloads ~4 real CSV.gz files (~30s depending on network).

Run: uv run --env-file=.env.local python scripts/demo_meteo_failures.py
"""

import tempfile
from pathlib import Path
from unittest.mock import patch

import httpx

from data_eng_etl_electricity_meteo.custom_downloads import meteo_climatologie as meteo_download

# Reduced set: 6 departments instead of 95 (fast demo)
_DEMO_DEPARTMENTS: set[str] = {"01", "02", "13", "69", "75", "93"}

_real_stream = meteo_download._stream_to_file


def _patched_stream(url: str, *, client: httpx.Client, path: Path) -> None:
    """Inject failures for specific departments via temp file name."""
    name = path.name

    # Dept 75: fail ALL downloads (parquet + CSV) → department skipped
    if name.startswith("75"):
        raise httpx.ConnectError(
            "Simulated: host unreachable",
            request=httpx.Request("GET", url),
        )

    # Dept 69: fail only Hydra parquet → CSV fallback
    if name.startswith("69") and "hydra" in name:
        raise httpx.ReadTimeout(
            "Simulated: Hydra timeout",
            request=httpx.Request("GET", url),
        )

    _real_stream(url, client=client, path=path)


def main() -> None:
    """Run a reduced climatologie download with simulated failures."""
    print("Downloading 6 departments (2 will fail)...")
    print("  - dept 75: both parquet + CSV fail → skipped")
    print("  - dept 69: parquet fails → CSV fallback (if API gave parquet URL)")
    print("  - dept 01, 02, 13, 93: normal download")
    print()

    with tempfile.TemporaryDirectory(prefix="demo_meteo_") as tmpdir:
        with (
            patch.object(meteo_download, "DEPARTMENTS", _DEMO_DEPARTMENTS),
            patch.object(meteo_download, "_stream_to_file", _patched_stream),
        ):
            try:
                result = meteo_download.download_climatologie(Path(tmpdir))
                print(f"\nMerged file: {result}")
                print(f"Size: {result.stat().st_size / 1024**2:.1f} MiB")
            except meteo_download.DownloadStageError as e:
                print(f"\nPipeline error: {e}")


if __name__ == "__main__":
    main()
