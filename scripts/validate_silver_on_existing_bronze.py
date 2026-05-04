"""Verify silver validation on existing bronze files.

Runs ``run_silver`` on the latest bronze for each registered dataset and reports
``SchemaValidationError`` / ``SourceSchemaDriftError`` / ``TransformValidationError``.
Used to detect whether tightened silver constraints (post-audit) reject any
already-present data.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

from data_eng_etl_electricity_meteo.core.exceptions import (
    BaseProjectException,
    SchemaValidationError,
)
from data_eng_etl_electricity_meteo.core.settings import settings
from data_eng_etl_electricity_meteo.transformations.registry import _REGISTRY as REGISTRY


def _latest_bronze(dataset_name: str) -> Path | None:
    """Return the most recent bronze parquet for *dataset_name*, or None."""
    bronze_dir = settings.data_dir_path / "bronze" / dataset_name
    if not bronze_dir.exists():
        return None
    candidates = sorted(bronze_dir.glob("*.parquet"))
    return candidates[-1] if candidates else None


def main() -> int:
    """Run silver validation per dataset; return non-zero on any failure."""
    print()
    failures = 0

    for name in sorted(REGISTRY):
        spec = REGISTRY[name]
        bronze_path = _latest_bronze(name)
        if bronze_path is None:
            print(f"  ⊘  {name:<32} — no bronze locally, skipped")
            continue

        t0 = time.perf_counter()
        try:
            df = spec.run_silver(bronze_path)
        except SchemaValidationError as err:
            failures += 1
            print(f"  ✗  {name:<32} — SchemaValidationError")
            for line in err.errors:
                print(f"        {line}")
            continue
        except BaseProjectException as err:
            failures += 1
            print(f"  ✗  {name:<32} — {type(err).__name__}: {err}")
            continue
        except Exception as err:
            failures += 1
            print(f"  ✗  {name:<32} — {type(err).__name__}: {err}")
            continue

        duration = time.perf_counter() - t0
        print(
            f"  ✓  {name:<32} — {len(df):>10,} rows × {len(df.columns):>2} cols ({duration:.1f}s)"
        )

    print()
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
