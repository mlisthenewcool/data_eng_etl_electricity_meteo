"""Dataset transform contract.

Defines ``DatasetTransformSpec`` — the immutable per-dataset contract bundling
bronze/silver transforms, source column sets, and silver schema.

Extracted from ``registry.py`` to avoid circular imports: both the registry and
individual transform modules import from this file.
"""

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from data_eng_etl_electricity_meteo.transformations.dataframe_model import DataFrameModel
from data_eng_etl_electricity_meteo.transformations.shared import (
    extract_diagnostics,
    prepare_silver,
    validate_not_empty,
    validate_source_columns,
)
from data_eng_etl_electricity_meteo.utils.polars import collect_narrow

# --------------------------------------------------------------------------------------
# Type aliases
# --------------------------------------------------------------------------------------


BronzeTransformFunc = Callable[[Path], pl.LazyFrame]

# Silver transforms receive a pre-processed LazyFrame (snake_case columns,
# all-null columns dropped) and return a LazyFrame with optional _diag_* / _warn_*
# diagnostic columns. The caller (run_silver) collects once, extracts diagnostics,
# selects schema columns, then validates.
SilverTransformFunc = Callable[[pl.LazyFrame], pl.LazyFrame]


# --------------------------------------------------------------------------------------
# Dataset transform contract
# --------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DatasetTransformSpec:
    """Immutable contract for a single dataset's transformations.

    Each dataset module defines a ``SPEC`` instance bundling all the pieces required by
    the pipeline. The registry collects them into a single dict.

    Parameters
    ----------
    name
        Dataset identifier (must match the catalog key).
    bronze_transform
        Landing file → bronze LazyFrame.
    silver_transform
        Pre-processed bronze LazyFrame → silver LazyFrame.
    all_source_columns
        Every column the source API returns (for schema drift detection).
    used_source_columns
        Columns the silver transform actually accesses
        (for documentation and future column-pruning optimization).
    silver_schema
        DataFrameModel subclass validating the silver output contract.
    """

    name: str
    bronze_transform: BronzeTransformFunc
    silver_transform: SilverTransformFunc
    all_source_columns: frozenset[str]
    used_source_columns: frozenset[str]
    silver_schema: type[DataFrameModel]

    def run_silver(self, bronze_path: Path) -> pl.DataFrame:
        """Read bronze parquet lazily, apply transforms, validate.

        Uses ``scan_parquet`` + lazy pipeline + single ``.collect()`` to avoid
        materializing multiple intermediate DataFrames.
        For the 18M-row climatologie dataset this reduces peak memory from ~5 GB
        (multiple eager copies) to ~2 GB (single Polars query plan).

        Parameters
        ----------
        bronze_path
            Path to the bronze parquet file (typically ``bronze_latest_path``).

        Returns
        -------
        pl.DataFrame
            Fully transformed and validated silver DataFrame.

        Raises
        ------
        SourceSchemaDriftError
            If source API columns have changed (added or removed).
        SchemaValidationError
            If silver output violates the declared schema contract.
        TransformValidationError
            If the transformed DataFrame is empty.
        polars.exceptions.PolarsError
            On any Polars read or compute failure.
        OSError
            If the bronze Parquet file cannot be read.
        """
        lf = pl.scan_parquet(bronze_path)
        lf = prepare_silver(lf, dataset_name=self.name, expected_columns=self.all_source_columns)
        validate_source_columns(
            lf, expected_columns=self.all_source_columns, dataset_name=self.name
        )
        lf = self.silver_transform(lf)
        df = collect_narrow(lf)
        df = extract_diagnostics(df)
        df = df.select(self.silver_schema.polars_schema().names())
        validate_not_empty(df, dataset_name=self.name)
        self.silver_schema.validate(df)
        return df
