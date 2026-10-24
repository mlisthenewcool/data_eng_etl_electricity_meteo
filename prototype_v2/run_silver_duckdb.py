"""Run the silver translation in pure DuckDB on local bronze files (validated runner).

Same steps as ``DatasetTransformSpec.run_silver``: prepare_silver
(snake_case + spurious all-null drop + drift check), transform, conditional dedup on PK
keeping the last row in source order, final column list from the reference output,
SilverSchema checks.

    python prototype_v2/run_silver_duckdb.py <bronze_dir> <reference_dir> <out_dir>
"""

import sys
import time
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).parent))
from silver_sql import CHECKS, DATASETS, GOLD, PK, prepare_columns, to_snake_case, transform

bronze_dir, reference_dir, out_dir = map(Path, sys.argv[1:4])
out_dir.mkdir(parents=True, exist_ok=True)

con = duckdb.connect()
con.execute("LOAD spatial")
con.execute("SET geometry_always_xy = true; SET enable_progress_bar = false")

for dataset in DATASETS:
    t = time.perf_counter()
    bronze = bronze_dir / f"{dataset}.parquet"
    cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{bronze}'").fetchall()]
    counts = dict(
        zip(
            cols,
            con.execute(
                "SELECT " + ", ".join(f'count("{c}")' for c in cols) + f" FROM '{bronze}'"
            ).fetchall()[0],
            strict=True,
        )
    )
    kept = prepare_columns(dataset, cols, counts)
    select_cols = ", ".join(f'"{c}" AS "{to_snake_case(c)}"' for c in kept)
    con.execute(f"""CREATE OR REPLACE TEMP VIEW src AS SELECT {select_cols},
        file_row_number AS _rn FROM read_parquet('{bronze}', file_row_number = true)""")
    con.execute(f"CREATE OR REPLACE TEMP VIEW t AS {transform(dataset)}")

    pk = ", ".join(PK[dataset])
    dups = con.execute(f"SELECT count(*) - count(DISTINCT ({pk})) FROM t").fetchall()[0][0]
    # Explicit column list = the silver contract (future dbt model select list).
    out_cols = [
        r[0]
        for r in con.execute(
            f"DESCRIBE SELECT * FROM '{reference_dir / f'{dataset}.parquet'}'"
        ).fetchall()
    ]
    final = "SELECT " + ", ".join(f'"{c}"' for c in out_cols) + " FROM t"
    if dups:
        final += f" QUALIFY row_number() OVER (PARTITION BY {pk} ORDER BY _rn DESC) = 1"
    target = out_dir / f"{dataset}.parquet"
    con.execute(f"COPY ({final}) TO '{target}' (FORMAT parquet)")
    violations = con.execute(f"SELECT {CHECKS[dataset]} FROM '{target}'").fetchall()[0][0]
    print(
        f"{dataset}: doublons retirés={dups} violations={violations} "
        f"en {time.perf_counter() - t:.2f} s"
    )

gold = GOLD.format(
    inst=f"'{out_dir}/odre_installations.parquet'",
    iris=f"'{out_dir}/ign_contours_iris.parquet'",
    st=f"'{out_dir}/meteo_france_stations.parquet'",
)
con.execute(f"COPY ({gold}) TO '{out_dir}/gold.parquet' (FORMAT parquet)")
print("gold:", con.execute(f"SELECT count(*) FROM '{out_dir}/gold.parquet'").fetchall()[0][0])
