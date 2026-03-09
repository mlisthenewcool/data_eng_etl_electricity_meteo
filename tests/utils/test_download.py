"""Unit tests for the download module."""

from unittest.mock import Mock

import pytest

from data_eng_etl_electricity_meteo.utils.download import _extract_filename, _short_url

# --------------------------------------------------------------------------------------
# _short_url
# --------------------------------------------------------------------------------------


class TestShortUrl:
    @pytest.mark.parametrize(
        argnames="url, expected",
        argvalues=[
            pytest.param(
                "https://data.example.fr/api/datasets/abc123/exports/parquet",
                "data.example.fr/…/abc123",
                id="skips_generic_segments",
            ),
            pytest.param(
                "https://host.fr/some/path/data.csv",
                "host.fr/…/data.csv",
                id="uses_last_meaningful_part",
            ),
            pytest.param(
                "https://host.fr/download",
                "host.fr/…/download",
                id="all_generic_falls_back_to_filename",
            ),
            pytest.param(
                "https://host.fr/path/with%20spaces/file.json",
                "host.fr/…/file.json",
                id="decodes_url_encoded",
            ),
        ],
    )
    def test_shortens_url(self, url: str, expected: str) -> None:
        assert _short_url(url) == expected


# --------------------------------------------------------------------------------------
# _extract_filename
# --------------------------------------------------------------------------------------


class TestExtractFilename:
    @staticmethod
    def _make_response(headers: dict[str, str]) -> Mock:
        response = Mock()
        response.headers = headers
        return response

    def test_extracts_from_content_disposition_quoted(self) -> None:
        response = self._make_response(
            {"content-disposition": 'attachment; filename="data.parquet"'}
        )
        assert _extract_filename(response, url="https://host.fr/download") == "data.parquet"

    def test_extracts_from_content_disposition_unquoted(self) -> None:
        response = self._make_response({"content-disposition": "attachment; filename=data.parquet"})
        assert _extract_filename(response, url="https://host.fr/dl") == "data.parquet"

    def test_extracts_from_url_path(self) -> None:
        response = self._make_response({})
        assert _extract_filename(response, url="https://host.fr/data/file.csv") == "file.csv"

    def test_strips_path_separators_for_security(self) -> None:
        response = self._make_response(
            {"content-disposition": 'attachment; filename="../../../etc/passwd"'}
        )
        assert _extract_filename(response, url="https://host.fr/dl") == "passwd"

    def test_url_path_rejects_extensionless_names(self) -> None:
        response = self._make_response({})
        assert _extract_filename(response, url="https://host.fr/api/download") is None

    def test_returns_none_when_no_filename(self) -> None:
        response = self._make_response({})
        assert _extract_filename(response, url="https://host.fr/api/v2/") is None
