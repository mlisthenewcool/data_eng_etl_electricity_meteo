"""Unit tests for the extraction module."""

from pathlib import Path

import pytest

from data_eng_etl_electricity_meteo.core.exceptions import FileIntegrityError
from data_eng_etl_electricity_meteo.utils.extraction import _validate_sqlite_header

# --------------------------------------------------------------------------------------
# _validate_sqlite_header
# --------------------------------------------------------------------------------------


class TestValidateSqliteHeader:
    def test_valid_header_passes(self, tmp_path: Path) -> None:
        path = tmp_path / "valid.gpkg"
        path.write_bytes(b"SQLite format 3\x00" + b"\x00" * 100)
        _validate_sqlite_header(path)  # should not raise

    def test_empty_file_raises(self, tmp_path: Path) -> None:
        path = tmp_path / "empty.gpkg"
        path.write_bytes(b"")
        with pytest.raises(FileIntegrityError) as exc_info:
            _validate_sqlite_header(path)
        assert "empty" in exc_info.value.reason.lower()

    def test_invalid_header_raises(self, tmp_path: Path) -> None:
        path = tmp_path / "bad.gpkg"
        path.write_bytes(b"NOT A SQLITE FILE!!")
        with pytest.raises(FileIntegrityError) as exc_info:
            _validate_sqlite_header(path)
        assert "invalid" in exc_info.value.reason.lower()

    def test_missing_file_raises(self, tmp_path: Path) -> None:
        path = tmp_path / "nonexistent.gpkg"
        with pytest.raises(FileIntegrityError) as exc_info:
            _validate_sqlite_header(path)
        assert "does not exist" in exc_info.value.reason.lower()
