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
    prepare_silver,
    validate_not_empty,
)

# --------------------------------------------------------------------------------------
# Type aliases
# --------------------------------------------------------------------------------------


BronzeTransformFunc = Callable[[Path], pl.LazyFrame]

# Silver transforms receive a pre-processed DataFrame (snake_case columns,
# all-null columns dropped) and return the transformed DataFrame.
SilverTransformFunc = Callable[[pl.DataFrame], pl.DataFrame]


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
        Pre-processed bronze DataFrame → silver DataFrame.
    all_source_columns
        Every column the source API returns (for schema drift detection).
    used_source_columns
        Columns the silver transform actually accesses
        (for documentation and future column-pruning optimisation).
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
        """Read bronze parquet, apply common + specific transforms, validate.

        Parameters
        ----------
        bronze_path
            Path to the bronze parquet file (typically ``bronze_latest_path``).

        Returns
        -------
        pl.DataFrame
            Fully transformed and validated silver DataFrame.
        """
        df = pl.read_parquet(bronze_path)
        df = prepare_silver(df, self.name, expected_columns=self.all_source_columns)
        df = self.silver_transform(df)
        validate_not_empty(df, self.name)
        return df
