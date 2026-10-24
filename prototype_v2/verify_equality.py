"""Row-by-row equality between a reference output and a candidate output.

The migration's acceptance criterion: every V2 silver/gold table must be identical to
the current pipeline's output (Polars silver, PostGIS/dbt gold).

Each side is a DuckDB relation expression, e.g. a Parquet file
(``'out/a/odre_installations.parquet'``), a DuckLake table (``lake.main_silver.x``) or a
Postgres table (``pg.silver.dim_installations``). Candidate columns are cast to the
reference types; extra candidate columns are ignored. Optional attachments (env):

- ``VERIFY_LAKE_CATALOG`` (``postgres:dbname=lake_catalog host=...``) -> ``lake``
- ``VERIFY_PG_DSN`` (libpq DSN) -> ``pg``
- ``S3_ENDPOINT``, ``S3_ACCESS_KEY_ID``, ``S3_SECRET_ACCESS_KEY``, ``S3_REGION``

    python prototype_v2/verify_equality.py "<reference relation>" "<candidate relation>"
"""

import os
import sys

import duckdb

ref, cand = sys.argv[1], sys.argv[2]
con = duckdb.connect()
# UTC session: comparing TIMESTAMPTZ through a local-time cast shifts values
# (false diffs).
con.execute("SET TimeZone = 'UTC'; SET enable_progress_bar = false")
if os.environ.get("S3_ENDPOINT"):
    con.execute("LOAD httpfs")
    con.execute(f"""CREATE SECRET (TYPE s3, KEY_ID '{os.environ["S3_ACCESS_KEY_ID"]}',
        SECRET '{os.environ["S3_SECRET_ACCESS_KEY"]}', ENDPOINT '{os.environ["S3_ENDPOINT"]}',
        URL_STYLE 'path', USE_SSL false, REGION '{os.environ.get("S3_REGION", "us-east-1")}')""")
if os.environ.get("VERIFY_LAKE_CATALOG"):
    con.execute("LOAD ducklake; LOAD postgres")
    con.execute(f"ATTACH 'ducklake:{os.environ['VERIFY_LAKE_CATALOG']}' AS lake (READ_ONLY)")
if os.environ.get("VERIFY_PG_DSN"):
    con.execute("LOAD postgres")
    con.execute(f"ATTACH '{os.environ['VERIFY_PG_DSN']}' AS pg (TYPE postgres, READ_ONLY)")

types = con.execute(f"DESCRIBE SELECT * FROM {ref}").fetchall()
ref_sel = ", ".join(f'"{c}"' for c, *_ in types)
cand_sel = ", ".join(f'CAST("{c}" AS {t}) AS "{c}"' for c, t, *_ in types)
n_ref = con.execute(f"SELECT count(*) FROM {ref}").fetchall()[0][0]
n_cand = con.execute(f"SELECT count(*) FROM {cand}").fetchall()[0][0]
only_ref = con.execute(
    f"SELECT count(*) FROM (SELECT {ref_sel} FROM {ref} EXCEPT ALL SELECT {cand_sel} FROM {cand})"
).fetchall()[0][0]
only_cand = con.execute(
    f"SELECT count(*) FROM (SELECT {cand_sel} FROM {cand} EXCEPT ALL SELECT {ref_sel} FROM {ref})"
).fetchall()[0][0]
identical = only_ref == only_cand == 0 and n_ref == n_cand
print(
    f"{'IDENTIQUE' if identical else 'DIFFÉRENT'} | lignes {n_ref} / {n_cand} | "
    f"seulement référence {only_ref} | seulement candidat {only_cand}"
)
sys.exit(0 if identical else 1)
