"""Unit tests for Météo France climatologie transforms."""

from pathlib import Path

import polars as pl
import pytest

from data_eng_etl_electricity_meteo.core.exceptions import SourceSchemaDriftError
from data_eng_etl_electricity_meteo.transformations.datasets.meteo_france_climatologie import (
    _ALL_SOURCE_COLUMNS,
    _BRONZE_COLUMNS,
    _COLUMNS_MAPPING,
    transform_bronze,
    transform_silver,
)
from data_eng_etl_electricity_meteo.transformations.shared import validate_source_columns
from data_eng_etl_electricity_meteo.utils.polars import collect_narrow

# --------------------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------------------


def _make_bronze_df(**overrides: list[object]) -> pl.DataFrame:
    """Build a minimal bronze-like DataFrame.

    Columns are snake_case (as after prepare_silver).
    Values are in final units as delivered by data.gouv.fr (°C, m/s, hPa, mm).

    Parameters
    ----------
    **overrides
        Column name → values list.
        Overrides the defaults for that column (e.g. ``t=[21.5, 9.8, -7.8]``).
    """
    defaults: dict[str, list[object]] = {
        "num_poste": ["01014001", "13055001", "75114001"],
        "aaaammjjhh": ["2026022800", "2026022801", "2026022802"],
        "glo": [100.0, 200.0, None],
        "ins": [30.0, 45.0, None],
        "n": [2.0, 5.0, None],
        "ff": [5.0, 12.0, None],
        "dd": [180.0, 270.0, None],
        "fxi": [8.0, 15.0, None],
        "t": [21.5, 9.8, None],
        "tx": [25.0, 14.5, None],
        "tn": [15.0, 5.0, None],
        "td": [12.0, 3.5, None],
        "u": [75.0, 60.0, None],
        "rr1": [0.0, 1.5, None],
        "pstat": [1013.25, 980.0, None],
        "pmer": [1013.25, 1010.0, None],
    }
    return pl.DataFrame(defaults | overrides)


def _write_landing_parquet(tmp_path: Path) -> Path:
    """Write a fake 196-column landing Parquet with String types."""
    # Build the 16 real columns as String (mimicking CSV infer_schema=False)
    data: dict[str, list[str | None]] = {
        "NUM_POSTE": ["01014001", "13055001"],
        "AAAAMMJJHH": ["2026022800", "2026022801"],
        "GLO": ["100", "200"],
        "INS": ["30", "45"],
        "N": ["2", "5"],
        "FF": ["5.0", "12.0"],
        "DD": ["180", "270"],
        "FXI": ["8.0", "15.0"],
        "T": ["21.5", "mq"],
        "TX": ["25.0", ""],
        "TN": ["15.0", "5.0"],
        "TD": ["12.0", "3.5"],
        "U": ["75", "60"],
        "RR1": ["0.0", "1.5"],
        "PSTAT": ["1013.25", "980.0"],
        "PMER": ["1013.25", "1010.0"],
    }
    # Add 180 extra columns to simulate the full 196-column schema
    for i in range(180):
        data[f"EXTRA_COL_{i}"] = ["x", "y"]

    df = pl.DataFrame(data)
    path = tmp_path / "landing.parquet"
    df.write_parquet(path)
    return path


@pytest.fixture
def bronze_df(tmp_path: Path) -> pl.DataFrame:
    """Collect the bronze transform from a fake 196-column landing Parquet."""
    path = _write_landing_parquet(tmp_path)
    return collect_narrow(transform_bronze(path))


# --------------------------------------------------------------------------------------
# Bronze transformation
# --------------------------------------------------------------------------------------


class TestBronzeTransform:
    def test_column_pruning(self, bronze_df: pl.DataFrame) -> None:
        assert len(bronze_df.columns) == 16
        assert set(bronze_df.columns) == set(_BRONZE_COLUMNS.keys())

    def test_numeric_columns_are_float64(self, bronze_df: pl.DataFrame) -> None:
        for col, expected_type in _BRONZE_COLUMNS.items():
            assert bronze_df[col].dtype == expected_type, f"Column {col} has wrong type"

    def test_sentinel_mq_becomes_null(self, bronze_df: pl.DataFrame) -> None:
        """Row 1 has "mq" for T in landing data."""
        assert bronze_df["T"][1] is None

    def test_empty_string_becomes_null(self, bronze_df: pl.DataFrame) -> None:
        """Row 1 has "" for TX in landing data."""
        assert bronze_df["TX"][1] is None

    def test_utf8_columns_preserved(self, bronze_df: pl.DataFrame) -> None:
        assert bronze_df["NUM_POSTE"].dtype == pl.Utf8
        assert bronze_df["AAAAMMJJHH"].dtype == pl.Utf8

    def test_typed_parquet_source(self, tmp_path: Path) -> None:
        """Hydra-generated Parquet files have inferred types (e.g. AAAAMMJJHH as Int64).

        Bronze must cast everything to the target schema regardless.
        """
        data: dict[str, list[str | int | float | None]] = {
            "NUM_POSTE": ["01014001", "13055001"],
            "AAAAMMJJHH": [2026022800, 2026022801],  # Int64, not String
            "GLO": [100.0, 200.0],
            "INS": [30.0, 45.0],
            "N": [2, 5],  # Int64
            "FF": [5.0, 12.0],
            "DD": [180, 270],  # Int64
            "FXI": [8.0, 15.0],
            "T": [21.5, 9.8],
            "TX": [25.0, 14.5],
            "TN": [15.0, 5.0],
            "TD": [12.0, 3.5],
            "U": [75, 60],  # Int64
            "RR1": [0.0, 1.5],
            "PSTAT": [1013.25, 980.0],
            "PMER": [1013.25, 1010.0],
        }
        # Add 180 extra columns
        for i in range(180):
            data[f"EXTRA_COL_{i}"] = [i, i + 1]

        path = tmp_path / "landing_typed.parquet"
        pl.DataFrame(data).write_parquet(path)
        result = collect_narrow(transform_bronze(path))

        for col, expected_type in _BRONZE_COLUMNS.items():
            assert result[col].dtype == expected_type, f"Column {col} has wrong type"

        # AAAAMMJJHH (originally Int64) should now be Utf8
        assert result["AAAAMMJJHH"].dtype == pl.Utf8
        assert result["AAAAMMJJHH"][0] == "2026022800"


# --------------------------------------------------------------------------------------
# Column selection and renaming
# --------------------------------------------------------------------------------------


class TestColumnSelection:
    def test_output_has_expected_columns(self) -> None:
        df = _make_bronze_df()
        result = collect_narrow(transform_silver(df.lazy()))

        expected_columns = list(_COLUMNS_MAPPING.values())
        schema_cols = [c for c in result.columns if not c.startswith("_")]
        assert schema_cols == expected_columns

    def test_output_column_count(self) -> None:
        df = _make_bronze_df()
        result = collect_narrow(transform_silver(df.lazy()))
        schema_cols = [c for c in result.columns if not c.startswith("_")]
        assert len(schema_cols) == 16

    def test_extra_source_columns_raise_drift_error(self) -> None:
        lf = (
            _make_bronze_df()
            .with_columns(
                pl.lit("extra_value").alias("extra_column"),
                pl.lit(42).alias("another_column"),
            )
            .lazy()
        )
        with pytest.raises(SourceSchemaDriftError) as exc_info:
            validate_source_columns(
                lf,
                expected_columns=_ALL_SOURCE_COLUMNS,
                dataset_name="meteo_france_climatologie",
            )
        assert "extra_column" in exc_info.value.added
        assert "another_column" in exc_info.value.added


# --------------------------------------------------------------------------------------
# Date parsing
# --------------------------------------------------------------------------------------


class TestDateParsing:
    def test_date_parsing(self) -> None:
        df = _make_bronze_df(aaaammjjhh=["2026022815", "2026010100", "2025123123"])
        result = collect_narrow(transform_silver(df.lazy()))

        result = result.sort("date_heure")
        dates = result["date_heure"].to_list()
        # Sorted: 2025-12-31, 2026-01-01, 2026-02-28
        assert dates[2].year == 2026
        assert dates[2].month == 2
        assert dates[2].day == 28
        assert dates[2].hour == 15

    def test_date_midnight(self) -> None:
        df = _make_bronze_df(aaaammjjhh=["2026010100", "2026020100", "2026030100"])
        result = collect_narrow(transform_silver(df.lazy()))

        result = result.sort("date_heure")
        dates = result["date_heure"].to_list()
        assert dates[0].hour == 0
        assert dates[0].day == 1
        assert dates[0].month == 1

    def test_date_column_is_datetime_utc(self) -> None:
        df = _make_bronze_df()
        result = collect_narrow(transform_silver(df.lazy()))
        assert result["date_heure"].dtype == pl.Datetime("us", "UTC")


# --------------------------------------------------------------------------------------
# Narrowing casts
# --------------------------------------------------------------------------------------


class TestNarrowingCasts:
    def test_nebulosite_is_int16(self) -> None:
        df = _make_bronze_df()
        result = collect_narrow(transform_silver(df.lazy()))
        assert result["nebulosite"].dtype == pl.Int16

    def test_direction_vent_is_int16(self) -> None:
        df = _make_bronze_df()
        result = collect_narrow(transform_silver(df.lazy()))
        assert result["direction_vent"].dtype == pl.Int16

    def test_humidite_is_int16(self) -> None:
        df = _make_bronze_df()
        result = collect_narrow(transform_silver(df.lazy()))
        assert result["humidite"].dtype == pl.Int16

    def test_null_narrowing(self) -> None:
        df = _make_bronze_df()
        result = collect_narrow(transform_silver(df.lazy()))

        # The third station (75114001) has None for all numeric columns
        row = result.filter(pl.col("id_station") == "75114001")
        assert row["nebulosite"].item() is None
        assert row["direction_vent"].item() is None
        assert row["humidite"].item() is None


# --------------------------------------------------------------------------------------
# Values passthrough (no unit conversion)
# --------------------------------------------------------------------------------------


class TestValuesPassthrough:
    def test_temperature_preserved(self) -> None:
        """No K*10 conversion — values pass through as-is from data.gouv.fr."""
        df = _make_bronze_df(t=[21.5, 9.8, -7.8])
        result = collect_narrow(transform_silver(df.lazy()))
        _inf = float("inf")
        temps = sorted(result["temperature"].to_list(), key=lambda x: x if x is not None else _inf)
        assert temps[0] == pytest.approx(-7.8)
        assert temps[1] == pytest.approx(9.8)
        assert temps[2] == pytest.approx(21.5)

    def test_wind_preserved(self) -> None:
        df = _make_bronze_df(ff=[5.0, 12.0, 0.6])
        result = collect_narrow(transform_silver(df.lazy()))
        _inf = float("inf")
        winds = sorted(result["vitesse_vent"].to_list(), key=lambda x: x if x is not None else _inf)
        assert winds[0] == pytest.approx(0.6)
        assert winds[1] == pytest.approx(5.0)
        assert winds[2] == pytest.approx(12.0)

    def test_pressure_preserved(self) -> None:
        df = _make_bronze_df(pstat=[1013.25, 980.0, 1020.5])
        result = collect_narrow(transform_silver(df.lazy()))
        pressures = sorted(
            result["pression_station"].to_list(),
            key=lambda x: x if x is not None else float("inf"),
        )
        assert pressures[0] == pytest.approx(980.0)
        assert pressures[1] == pytest.approx(1013.25)
        assert pressures[2] == pytest.approx(1020.5)

    def test_precipitation_preserved(self) -> None:
        df = _make_bronze_df(rr1=[0.0, 1.5, 12.4])
        result = collect_narrow(transform_silver(df.lazy()))
        precips = sorted(
            result["precipitations"].to_list(),
            key=lambda x: x if x is not None else float("inf"),
        )
        assert precips[0] == pytest.approx(0.0)
        assert precips[1] == pytest.approx(1.5)
        assert precips[2] == pytest.approx(12.4)


# --------------------------------------------------------------------------------------
# id_station type handling
# --------------------------------------------------------------------------------------


class TestIdStationType:
    def test_id_station_is_string(self) -> None:
        df = _make_bronze_df()
        result = collect_narrow(transform_silver(df.lazy()))
        assert result["id_station"].dtype == pl.Utf8

    def test_leading_zeros_preserved(self) -> None:
        df = _make_bronze_df(num_poste=["01014001", "02345002", "03456003"])
        result = collect_narrow(transform_silver(df.lazy()))
        ids = sorted(result["id_station"].to_list())
        assert ids[0] == "01014001"
        assert ids[1] == "02345002"


# --------------------------------------------------------------------------------------
# Row count preservation
# --------------------------------------------------------------------------------------


class TestRowCount:
    def test_row_count_preserved(self) -> None:
        df = _make_bronze_df()
        result = collect_narrow(transform_silver(df.lazy()))
        assert len(result) == len(df)


# --------------------------------------------------------------------------------------
# Deduplication
# --------------------------------------------------------------------------------------


class TestDeduplication:
    def test_duplicates_preserved_by_transform(self) -> None:
        """transform_silver preserves duplicates (dedup is in run_silver)."""
        df = _make_bronze_df(
            num_poste=["01014001", "01014001", "13055001"],
            aaaammjjhh=["2026022800", "2026022800", "2026022801"],
        )
        result = collect_narrow(transform_silver(df.lazy()))
        assert len(result) == 3
