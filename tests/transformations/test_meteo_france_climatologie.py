"""Unit tests for Météo France climatologie transforms."""

import tempfile
from pathlib import Path

import polars as pl
import pytest

from data_eng_etl_electricity_meteo.core.exceptions import SourceSchemaDriftError
from data_eng_etl_electricity_meteo.transformations.meteo_france_climatologie import (
    BRONZE_COLUMNS,
    COLUMNS_MAPPING,
    transform_bronze,
    transform_silver,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_bronze_df(  # noqa: PLR0913
    num_poste: list[str] | None = None,
    aaaammjjhh: list[str] | None = None,
    t: list[float | None] | None = None,
    ff: list[float | None] | None = None,
    pstat: list[float | None] | None = None,
    rr1: list[float | None] | None = None,
) -> pl.DataFrame:
    """Build a minimal bronze-like DataFrame.

    Columns are snake_case (as after prepare_silver).
    Values are in final units as delivered by data.gouv.fr (°C, m/s, hPa, mm).
    """
    data: dict[str, list] = {
        "num_poste": num_poste or ["01014001", "13055001", "75114001"],
        "aaaammjjhh": aaaammjjhh or ["2026022800", "2026022801", "2026022802"],
        "glo": [100.0, 200.0, None],
        "ins": [30.0, 45.0, None],
        "n": [2.0, 5.0, None],
        "ff": ff or [5.0, 12.0, None],
        "dd": [180.0, 270.0, None],
        "fxi": [8.0, 15.0, None],
        "t": t or [21.5, 9.8, None],
        "tx": [25.0, 14.5, None],
        "tn": [15.0, 5.0, None],
        "td": [12.0, 3.5, None],
        "u": [75.0, 60.0, None],
        "rr1": rr1 or [0.0, 1.5, None],
        "pstat": pstat or [1013.25, 980.0, None],
        "pmer": [1013.25, 1010.0, None],
    }
    return pl.DataFrame(data)


# ---------------------------------------------------------------------------
# Bronze transformation
# ---------------------------------------------------------------------------


class TestBronzeTransform:
    """Tests for the bronze transform: column pruning and typing."""

    def _write_landing_parquet(self, tmp_path: Path) -> Path:
        """Write a fake 196-column landing parquet with String types."""
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

    def test_column_pruning(self) -> None:
        """Bronze should select only the 16 columns from BRONZE_COLUMNS."""
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write_landing_parquet(Path(tmp))
            result = transform_bronze(path).collect()
            assert isinstance(result, pl.DataFrame)

        assert len(result.columns) == 16
        assert set(result.columns) == set(BRONZE_COLUMNS.keys())

    def test_numeric_columns_are_float64(self) -> None:
        """Numeric columns should be cast to Float64."""
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write_landing_parquet(Path(tmp))
            result = transform_bronze(path).collect()
            assert isinstance(result, pl.DataFrame)

        for col, expected_type in BRONZE_COLUMNS.items():
            assert result[col].dtype == expected_type, f"Column {col} has wrong type"

    def test_sentinel_mq_becomes_null(self) -> None:
        """Sentinel string "mq" should become null after strict=False cast."""
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write_landing_parquet(Path(tmp))
            result = transform_bronze(path).collect()
            assert isinstance(result, pl.DataFrame)

        # Row 1 has "mq" for T → should be null
        assert result["T"][1] is None

    def test_empty_string_becomes_null(self) -> None:
        """Empty string "" should become null after strict=False cast."""
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write_landing_parquet(Path(tmp))
            result = transform_bronze(path).collect()
            assert isinstance(result, pl.DataFrame)

        # Row 1 has "" for TX → should be null
        assert result["TX"][1] is None

    def test_utf8_columns_preserved(self) -> None:
        """Utf8 columns (NUM_POSTE, AAAAMMJJHH) should remain as Utf8."""
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write_landing_parquet(Path(tmp))
            result = transform_bronze(path).collect()
            assert isinstance(result, pl.DataFrame)

        assert result["NUM_POSTE"].dtype == pl.Utf8
        assert result["AAAAMMJJHH"].dtype == pl.Utf8

    def test_typed_parquet_source(self) -> None:
        """Parquet with typed columns (Int64/Float64) should produce same result.

        Hydra-generated Parquet files have inferred types (e.g. AAAAMMJJHH as Int64).
        Bronze must cast everything to the target schema regardless.
        """
        data: dict[str, list] = {
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

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "landing_typed.parquet"
            pl.DataFrame(data).write_parquet(path)
            result = transform_bronze(path).collect()
            assert isinstance(result, pl.DataFrame)

        # All columns should match target types
        for col, expected_type in BRONZE_COLUMNS.items():
            assert result[col].dtype == expected_type, f"Column {col} has wrong type"

        # AAAAMMJJHH (originally Int64) should now be Utf8
        assert result["AAAAMMJJHH"].dtype == pl.Utf8
        assert result["AAAAMMJJHH"][0] == "2026022800"


# ---------------------------------------------------------------------------
# Column selection and renaming
# ---------------------------------------------------------------------------


class TestColumnSelection:
    """Tests for column selection and renaming in silver transform."""

    def test_output_has_expected_columns(self) -> None:
        """Silver output should have exactly the 16 mapped columns."""
        df = _make_bronze_df()
        result = transform_silver(df)

        expected_columns = list(COLUMNS_MAPPING.values())
        assert result.columns == expected_columns

    def test_output_column_count(self) -> None:
        """Silver output should have exactly 16 columns."""
        df = _make_bronze_df()
        result = transform_silver(df)
        assert len(result.columns) == 16

    def test_extra_source_columns_raise_drift_error(self) -> None:
        """Extra source columns should raise SourceSchemaDriftError."""
        df = _make_bronze_df().with_columns(
            pl.lit("extra_value").alias("extra_column"),
            pl.lit(42).alias("another_column"),
        )
        with pytest.raises(SourceSchemaDriftError) as exc_info:
            transform_silver(df)
        assert "extra_column" in exc_info.value.added
        assert "another_column" in exc_info.value.added


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------


class TestDateParsing:
    """Tests for AAAAMMJJHH → datetime conversion."""

    def test_date_parsing(self) -> None:
        """String date "2026022815" should parse to 2026-02-28T15:00:00 UTC."""
        df = _make_bronze_df(aaaammjjhh=["2026022815", "2026010100", "2025123123"])
        result = transform_silver(df)

        dates = result["date_heure"].to_list()
        assert dates[0].year == 2026
        assert dates[0].month == 2
        assert dates[0].day == 28
        assert dates[0].hour == 15

    def test_date_midnight(self) -> None:
        """Hour 00 should parse correctly (no leading zero loss)."""
        df = _make_bronze_df(aaaammjjhh=["2026010100", "2026020100", "2026030100"])
        result = transform_silver(df)

        dates = result["date_heure"].to_list()
        assert dates[0].hour == 0
        assert dates[0].day == 1
        assert dates[0].month == 1

    def test_date_column_is_datetime_type(self) -> None:
        """date_heure should be Datetime type with UTC timezone."""
        df = _make_bronze_df()
        result = transform_silver(df)

        assert result["date_heure"].dtype == pl.Datetime("us", "UTC")


# ---------------------------------------------------------------------------
# Narrowing casts
# ---------------------------------------------------------------------------


class TestNarrowingCasts:
    """Tests for Int16 narrowing casts on integer-valued columns."""

    def test_nebulosite_is_int16(self) -> None:
        """Nébulosité should be narrowed to Int16."""
        df = _make_bronze_df()
        result = transform_silver(df)
        assert result["nebulosite"].dtype == pl.Int16

    def test_direction_vent_is_int16(self) -> None:
        """Direction du vent should be narrowed to Int16."""
        df = _make_bronze_df()
        result = transform_silver(df)
        assert result["direction_vent"].dtype == pl.Int16

    def test_humidite_is_int16(self) -> None:
        """Humidité should be narrowed to Int16."""
        df = _make_bronze_df()
        result = transform_silver(df)
        assert result["humidite"].dtype == pl.Int16

    def test_null_narrowing(self) -> None:
        """Null values should remain null after narrowing cast."""
        df = _make_bronze_df()
        result = transform_silver(df)

        # Row 2 (index 2) has None for all numeric columns
        assert result["nebulosite"][2] is None
        assert result["direction_vent"][2] is None
        assert result["humidite"][2] is None


# ---------------------------------------------------------------------------
# Values passthrough (no unit conversion)
# ---------------------------------------------------------------------------


class TestValuesPassthrough:
    """Tests that values pass through unchanged (no unit conversion)."""

    def test_temperature_preserved(self) -> None:
        """Temperature 21.5°C should remain 21.5 (no K×10 conversion)."""
        df = _make_bronze_df(t=[21.5, 9.8, -7.8])
        result = transform_silver(df)
        assert result["temperature"][0] == pytest.approx(21.5)
        assert result["temperature"][1] == pytest.approx(9.8)
        assert result["temperature"][2] == pytest.approx(-7.8)

    def test_wind_preserved(self) -> None:
        """Wind speed 5.0 m/s should remain 5.0 (no ÷10 conversion)."""
        df = _make_bronze_df(ff=[5.0, 12.0, 0.6])
        result = transform_silver(df)
        assert result["vitesse_vent"][0] == pytest.approx(5.0)
        assert result["vitesse_vent"][1] == pytest.approx(12.0)
        assert result["vitesse_vent"][2] == pytest.approx(0.6)

    def test_pressure_preserved(self) -> None:
        """Pressure 1013.25 hPa should remain 1013.25 (no Pa→hPa conversion)."""
        df = _make_bronze_df(pstat=[1013.25, 980.0, 1020.5])
        result = transform_silver(df)
        assert result["pression_station"][0] == pytest.approx(1013.25)
        assert result["pression_station"][1] == pytest.approx(980.0)
        assert result["pression_station"][2] == pytest.approx(1020.5)

    def test_precipitation_preserved(self) -> None:
        """Precipitation 1.5 mm should remain 1.5 (no ÷10 conversion)."""
        df = _make_bronze_df(rr1=[0.0, 1.5, 12.4])
        result = transform_silver(df)
        assert result["precipitations"][0] == pytest.approx(0.0)
        assert result["precipitations"][1] == pytest.approx(1.5)
        assert result["precipitations"][2] == pytest.approx(12.4)


# ---------------------------------------------------------------------------
# id_station type handling
# ---------------------------------------------------------------------------


class TestIdStationType:
    """Tests for id_station type casting."""

    def test_id_station_is_string(self) -> None:
        """id_station should always be string type."""
        df = _make_bronze_df()
        result = transform_silver(df)
        assert result["id_station"].dtype == pl.Utf8

    def test_numeric_station_id_preserved(self) -> None:
        """Station IDs with leading zeros should be preserved as strings."""
        df = _make_bronze_df(num_poste=["01014001", "02345002", "03456003"])
        result = transform_silver(df)
        assert result["id_station"][0] == "01014001"
        assert result["id_station"][1] == "02345002"


# ---------------------------------------------------------------------------
# Row count preservation
# ---------------------------------------------------------------------------


class TestRowCount:
    """Tests for row count after transform."""

    def test_row_count_preserved(self) -> None:
        """Row count should be preserved (no filtering)."""
        df = _make_bronze_df()
        result = transform_silver(df)
        assert len(result) == len(df)
