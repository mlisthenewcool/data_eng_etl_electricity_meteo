"""DuckLake administration for the V2 prototype: init (lake + schemas) and maintenance.

``init``: creates the lake (DATA_PATH is only accepted at creation; dbt v2 does not
pass it) and pre-creates the dbt schemas (dbt v2 workaround: worker connections do not
see schemas created concurrently by other threads, see dbt-labs/dbt PR #16567).

``maintain``: expires old snapshots, deletes unreferenced files, merges small files.
DuckLake never rewrites Parquet in place: without maintenance, every rebuild keeps its
files (12 prototype runs grew the lake to 5.9 GB).

Environment:

- LAKE_CATALOG: ``postgres:dbname=... host=... port=... user=... password=...``
- LAKE_DATA_PATH: ``s3://bucket/lake/``
- S3_ENDPOINT, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, S3_REGION
- LAKE_RETENTION: DuckDB interval, default ``7 days``

    python prototype_v2/lake_admin.py init main_bronze main_silver main_gold python
    prototype_v2/lake_admin.py maintain
"""

import os
import sys

import duckdb

action, schemas = sys.argv[1], sys.argv[2:]
con = duckdb.connect()
con.execute("LOAD httpfs; LOAD postgres; LOAD ducklake")
con.execute(f"""CREATE SECRET (TYPE s3, KEY_ID '{os.environ["S3_ACCESS_KEY_ID"]}',
    SECRET '{os.environ["S3_SECRET_ACCESS_KEY"]}', ENDPOINT '{os.environ["S3_ENDPOINT"]}',
    URL_STYLE 'path', USE_SSL false, REGION '{os.environ.get("S3_REGION", "us-east-1")}')""")
catalog = os.environ["LAKE_CATALOG"]

if action == "init":
    data_path = os.environ["LAKE_DATA_PATH"]
    con.execute(f"ATTACH 'ducklake:{catalog}' AS lake (DATA_PATH '{data_path}')")
    for schema in schemas:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS lake.{schema}")
    print("lac prêt, schémas :", ", ".join(schemas))
elif action == "maintain":
    retention = os.environ.get("LAKE_RETENTION", "7 days")
    con.execute(f"ATTACH 'ducklake:{catalog}' AS lake")
    con.execute(
        f"CALL ducklake_expire_snapshots('lake', older_than => now() - INTERVAL '{retention}')"
    )
    removed = con.execute(
        f"CALL ducklake_cleanup_old_files('lake', older_than => now() - INTERVAL '{retention}')"
    ).fetchall()
    con.execute("CALL ducklake_merge_adjacent_files('lake')")
    print(f"maintenance OK, fichiers supprimés : {len(removed)}")
else:
    sys.exit(f"action inconnue : {action}")
