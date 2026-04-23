"""Unit tests for the Postgres silver-layer loader helpers."""

from dataclasses import dataclass
from typing import Any, cast

import psycopg
import pytest

from data_eng_etl_electricity_meteo.core.exceptions import PostgresLoadError
from data_eng_etl_electricity_meteo.loaders.pg_loader import _fetch_scalar_int


@dataclass
class _StubCursor:
    """Minimal cursor stub exposing only ``fetchone()``, the method the helper uses."""

    fetchone_value: object

    def fetchone(self) -> object:
        return self.fetchone_value


def _make_cursor(fetchone_value: object) -> psycopg.Cursor[Any]:
    """Build a stub cursor that returns ``fetchone_value`` on ``fetchone()``."""
    return cast("psycopg.Cursor[Any]", _StubCursor(fetchone_value))


# --------------------------------------------------------------------------------------
# _fetch_scalar_int
# --------------------------------------------------------------------------------------


class TestFetchScalarInt:
    """Contract: exactly 1 row with exactly 1 int column, else PostgresLoadError."""

    @pytest.mark.parametrize(
        argnames="value, expected",
        argvalues=[
            pytest.param(42, 42, id="small_int"),
            pytest.param(0, 0, id="zero_from_empty_table_count"),
            pytest.param(10_000_000_000, 10_000_000_000, id="bigint_range"),
        ],
    )
    def test_returns_int(self, value: int, expected: int) -> None:
        assert _fetch_scalar_int(_make_cursor((value,))) == expected

    def test_raises_on_no_row(self) -> None:
        with pytest.raises(PostgresLoadError, match="no row"):
            _fetch_scalar_int(_make_cursor(None))

    @pytest.mark.parametrize(
        argnames="fetched",
        argvalues=[
            pytest.param((), id="empty_tuple"),
            pytest.param((1, 2), id="multiple_columns"),
            pytest.param(("42",), id="str_value"),
            pytest.param((None,), id="none_value"),
            pytest.param((42.0,), id="float_value"),
        ],
    )
    def test_raises_on_unexpected_shape(self, fetched: object) -> None:
        with pytest.raises(PostgresLoadError, match="unexpected shape"):
            _fetch_scalar_int(_make_cursor(fetched))

    @pytest.mark.parametrize(
        argnames="fetched",
        argvalues=[pytest.param((True,), id="true"), pytest.param((False,), id="false")],
    )
    def test_raises_on_bool(self, fetched: object) -> None:
        # ``bool`` subclasses ``int``, so without the explicit ``bool`` arm the helper
        # would silently accept a Postgres ``boolean`` column and return True/False.
        with pytest.raises(PostgresLoadError, match="bool, not int"):
            _fetch_scalar_int(_make_cursor(fetched))
