"""Produce the reference silver outputs with the current Polars code.

Run it BEFORE removing Polars.

Writes ``<out_dir>/<dataset>.parquet`` for each dataset, from the bronze files given in
``<bronze_dir>/<dataset>.parquet``. Run with the project environment:

    uv run python prototype_v2/reference_polars.py <bronze_dir> <out_dir>
"""

import sys
import time
from pathlib import Path

from data_eng_etl_electricity_meteo.transformations.registry import get_transform_spec

sys.path.insert(0, str(Path(__file__).parent))
from silver_sql import DATASETS

bronze_dir, out_dir = Path(sys.argv[1]), Path(sys.argv[2])
out_dir.mkdir(parents=True, exist_ok=True)
for dataset in DATASETS:
    t = time.perf_counter()
    df = get_transform_spec(dataset).run_silver(bronze_dir / f"{dataset}.parquet")
    df.write_parquet(out_dir / f"{dataset}.parquet")
    print(f"{dataset}: {df.height} lignes en {time.perf_counter() - t:.2f} s")
