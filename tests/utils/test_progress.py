"""Unit tests for the progress module."""

from unittest.mock import patch

from data_eng_etl_electricity_meteo.utils.progress import ThrottledProgressTracker

# --------------------------------------------------------------------------------------
# ThrottledProgressTracker.accumulate
# --------------------------------------------------------------------------------------


class TestThrottledProgressTrackerAccumulate:
    def test_first_call_does_not_trigger(self) -> None:
        tracker = ThrottledProgressTracker(total=100)
        assert not tracker.accumulate(1)

    def test_time_trigger_after_interval(self) -> None:
        tracker = ThrottledProgressTracker(total=1000)
        tracker.accumulate(1)

        with patch("data_eng_etl_electricity_meteo.utils.progress.time") as mock_time:
            mock_time.monotonic.return_value = tracker._last_log_time + 11.0
            assert tracker.accumulate(1)

    def test_pct_trigger_after_threshold(self) -> None:
        tracker = ThrottledProgressTracker(total=100)
        tracker.accumulate(1)

        with patch("data_eng_etl_electricity_meteo.utils.progress.time") as mock_time:
            # Past LOG_MIN_INTERVAL_S (5s) but not LOG_INTERVAL_S (10s)
            mock_time.monotonic.return_value = tracker._last_log_time + 6.0
            assert tracker.accumulate(10)  # 11% > LOG_INTERVAL_PCT (10%)

    def test_min_interval_guards_pct_trigger(self) -> None:
        tracker = ThrottledProgressTracker(total=100)
        tracker.accumulate(1)

        with patch("data_eng_etl_electricity_meteo.utils.progress.time") as mock_time:
            mock_time.monotonic.return_value = tracker._last_log_time + 2.0
            # 16% change but elapsed < LOG_MIN_INTERVAL_S
            assert not tracker.accumulate(15)

    def test_unknown_total_disables_pct_trigger(self) -> None:
        tracker = ThrottledProgressTracker(total=0)
        tracker.accumulate(1)

        with patch("data_eng_etl_electricity_meteo.utils.progress.time") as mock_time:
            mock_time.monotonic.return_value = tracker._last_log_time + 4.0
            # No time trigger (< 10s), no pct trigger (total unknown)
            assert not tracker.accumulate(999)


# --------------------------------------------------------------------------------------
# ThrottledProgressTracker properties
# --------------------------------------------------------------------------------------


class TestThrottledProgressTrackerProperties:
    def test_processed_mib(self) -> None:
        tracker = ThrottledProgressTracker(total=0)
        tracker.accumulate(1024**2 * 5)
        assert tracker.processed_mib == 5.0

    def test_total_mib_none_when_unknown(self) -> None:
        tracker = ThrottledProgressTracker(total=0)
        assert tracker.total_mib is None

    def test_pct_none_when_total_unknown(self) -> None:
        tracker = ThrottledProgressTracker(total=0)
        tracker.accumulate(50)
        assert tracker.pct is None

    def test_pct_calculation(self) -> None:
        tracker = ThrottledProgressTracker(total=200)
        tracker.accumulate(50)
        assert tracker.pct == 25.0
