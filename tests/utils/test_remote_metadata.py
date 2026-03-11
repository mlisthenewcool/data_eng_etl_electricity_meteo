"""Unit tests for the remote_metadata module."""

from datetime import UTC, datetime

from data_eng_etl_electricity_meteo.utils.remote_metadata import (
    ChangeDetectionResult,
    RemoteFileMetadata,
)

# --------------------------------------------------------------------------------------
# ChangeDetectionResult
# --------------------------------------------------------------------------------------


class TestChangeDetectionResult:
    def test_bool_true_when_changed(self) -> None:
        result = ChangeDetectionResult(has_changed=True, reason="test")
        assert result

    def test_bool_false_when_unchanged(self) -> None:
        result = ChangeDetectionResult(has_changed=False, reason="test")
        assert not result


# --------------------------------------------------------------------------------------
# RemoteFileMetadata.compare_with — ETag
# --------------------------------------------------------------------------------------


class TestCompareWithEtag:
    def test_identical_etag_reports_no_change(self) -> None:
        current = RemoteFileMetadata(etag="abc123")
        previous = RemoteFileMetadata(etag="abc123")
        result = current.compare_with(previous)
        assert not result
        assert "identical" in result.reason.lower()

    def test_different_etag_reports_change(self) -> None:
        current = RemoteFileMetadata(etag="new")
        previous = RemoteFileMetadata(etag="old")
        result = current.compare_with(previous)
        assert result
        assert "changed" in result.reason.lower()

    def test_etag_takes_priority_over_last_modified(self) -> None:
        current = RemoteFileMetadata(
            etag="same",
            last_modified=datetime(2026, 3, 8, tzinfo=UTC),
        )
        previous = RemoteFileMetadata(
            etag="same",
            last_modified=datetime(2025, 1, 1, tzinfo=UTC),
        )
        result = current.compare_with(previous)
        assert not result

    def test_missing_etag_on_one_side_falls_through(self) -> None:
        current = RemoteFileMetadata(etag="abc", content_length=100)
        previous = RemoteFileMetadata(content_length=100)
        result = current.compare_with(previous)
        assert not result
        assert "size" in result.reason.lower()


# --------------------------------------------------------------------------------------
# RemoteFileMetadata.compare_with — Last-Modified
# --------------------------------------------------------------------------------------


class TestCompareWithLastModified:
    def test_identical_date_reports_no_change(self) -> None:
        dt = datetime(2026, 1, 1, tzinfo=UTC)
        current = RemoteFileMetadata(last_modified=dt)
        previous = RemoteFileMetadata(last_modified=dt)
        result = current.compare_with(previous)
        assert not result
        assert "identical" in result.reason.lower()

    def test_newer_date_reports_change(self) -> None:
        current = RemoteFileMetadata(last_modified=datetime(2026, 3, 8, tzinfo=UTC))
        previous = RemoteFileMetadata(last_modified=datetime(2025, 1, 1, tzinfo=UTC))
        result = current.compare_with(previous)
        assert result
        assert "newer" in result.reason.lower()

    def test_older_date_reports_rollback(self) -> None:
        current = RemoteFileMetadata(last_modified=datetime(2024, 1, 1, tzinfo=UTC))
        previous = RemoteFileMetadata(last_modified=datetime(2026, 3, 8, tzinfo=UTC))
        result = current.compare_with(previous)
        assert result
        assert "rollback" in result.reason.lower()


# --------------------------------------------------------------------------------------
# RemoteFileMetadata.compare_with — Content-Length
# --------------------------------------------------------------------------------------


class TestCompareWithContentLength:
    def test_identical_size_reports_no_change(self) -> None:
        current = RemoteFileMetadata(content_length=1024)
        previous = RemoteFileMetadata(content_length=1024)
        result = current.compare_with(previous)
        assert not result
        assert "identical" in result.reason.lower()

    def test_different_size_reports_change(self) -> None:
        current = RemoteFileMetadata(content_length=2048)
        previous = RemoteFileMetadata(content_length=1024)
        result = current.compare_with(previous)
        assert result
        assert "changed" in result.reason.lower()


# --------------------------------------------------------------------------------------
# RemoteFileMetadata.compare_with — Edge cases
# --------------------------------------------------------------------------------------


class TestCompareWithEdgeCases:
    def test_no_metadata_on_current_reports_change(self) -> None:
        current = RemoteFileMetadata()
        previous = RemoteFileMetadata(etag="abc")
        result = current.compare_with(previous)
        assert result
        assert "missing" in result.reason.lower()

    def test_no_metadata_on_previous_reports_change(self) -> None:
        current = RemoteFileMetadata(etag="abc")
        previous = RemoteFileMetadata()
        result = current.compare_with(previous)
        assert result
        assert "missing" in result.reason.lower()

    def test_no_overlapping_fields_reports_change(self) -> None:
        current = RemoteFileMetadata(etag="abc")
        previous = RemoteFileMetadata(content_length=100)
        result = current.compare_with(previous)
        assert result
        assert "no matching" in result.reason.lower()
