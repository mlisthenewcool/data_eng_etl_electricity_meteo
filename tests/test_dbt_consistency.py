"""Validate that Python SilverSchema constraints are mirrored in dbt sources.

Architecture: defense-in-depth validation in two layers:

- **Python DataFrameModel** (pre-write): source of truth for all column constraints
  (nullable, unique, isin, bounds). Validates before writing Parquet to silver.
- **dbt _sources.yml** (post-load): safety net that re-checks primary key constraints
  (unique + not_null) after loading into Postgres.
- **This test**: ensures the two layers stay synchronized. Every ``nullable=False`` or
  ``unique=True`` in a SilverSchema must have a matching ``not_null`` / ``unique`` test
  in ``_sources.yml``. ``isin`` constraints must have an ``accepted_values`` test.

Bounds (ge/le/gt/lt) are Python-only — not checked here.
Cross-dataset tests (relationships) are manual in dbt — not checked here.
"""

from collections.abc import Callable

import pytest
import yaml

from data_eng_etl_electricity_meteo.core.data_catalog import DataCatalog, RemoteDatasetConfig
from data_eng_etl_electricity_meteo.core.settings import settings
from data_eng_etl_electricity_meteo.transformations.dataframe_model import Column
from data_eng_etl_electricity_meteo.transformations.registry import get_transform_spec

# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------


def _load_dbt_sources() -> dict[str, dict[str, list[str]]]:
    """Parse ``_sources.yml`` into ``{table_name: {column_name: [test_names]}}``.

    Returns
    -------
    dict[str, dict[str, list[str]]]
        Mapping of table name → column name → list of dbt test names.
    """
    sources_path = settings.dbt_project_dir / "models" / "silver" / "_sources.yml"
    raw = yaml.safe_load(sources_path.read_text())

    result: dict[str, dict[str, list[str]]] = {}
    for source in raw.get("sources", []):
        for table in source.get("tables", []):
            table_name = table["name"]
            columns: dict[str, list[str]] = {}
            for col in table.get("columns", []):
                col_name = col["name"]
                test_names: list[str] = []
                for test in col.get("tests", []):
                    if isinstance(test, str):
                        test_names.append(test)
                    elif isinstance(test, dict):
                        # e.g. {accepted_values: {values: [...]}}
                        test_names.extend(test.keys())
                columns[col_name] = test_names
            result[table_name] = columns

    return result


try:
    _catalog = DataCatalog.load(settings.data_catalog_file_path)
    _remote_datasets = _catalog.get_remote_datasets()
    _dbt_sources = _load_dbt_sources()
except Exception as exc:
    pytest.skip(f"Cannot load catalog or dbt sources: {exc}", allow_module_level=True)


# --------------------------------------------------------------------------------------
# Constraint → dbt test mapping
# --------------------------------------------------------------------------------------

# Each tuple: (predicate on Column, dbt test name, human label for error messages).
_CONSTRAINT_TO_DBT: list[tuple[Callable[[Column], bool], str, str]] = [
    (lambda c: not c.nullable, "not_null", "nullable=False"),
    (lambda c: c.unique, "unique", "unique=True"),
    (lambda c: c.isin is not None, "accepted_values", "isin"),
]


# --------------------------------------------------------------------------------------
# Tests
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize("dataset", _remote_datasets, ids=lambda d: d.name)
@pytest.mark.parametrize(
    argnames="predicate, dbt_test, label",
    argvalues=_CONSTRAINT_TO_DBT,
    ids=["not_null", "unique", "accepted_values"],
)
def test_constraint_mirrored_in_dbt(
    dataset: RemoteDatasetConfig,
    predicate: Callable[[Column], bool],
    dbt_test: str,
    label: str,
) -> None:
    """Every SilverSchema constraint has a matching dbt test in ``_sources.yml``."""
    spec = get_transform_spec(dataset.name)
    table_name = dataset.postgres.table
    dbt_columns = _dbt_sources.get(table_name, {})

    for col_name, resolved in spec.silver_schema.__columns__.items():
        if predicate(resolved.constraints):
            dbt_tests = dbt_columns.get(col_name, [])
            assert dbt_test in dbt_tests, (
                f"Column '{col_name}' is {label} in {spec.name} SilverSchema "
                f"but has no '{dbt_test}' test in _sources.yml "
                f"(table '{table_name}')"
            )
