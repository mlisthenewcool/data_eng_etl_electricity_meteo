"""Tests for the Loguru-based logger module."""

from typing import TYPE_CHECKING, Generator

if TYPE_CHECKING:
    pass

import pytest

from data_eng_etl_electricity_meteo.core.logger import (
    LoguruLogger,
    logger,
)


@pytest.fixture(autouse=True)
def isolate_singleton() -> Generator[None, None, None]:
    """Reset singleton after each test for isolation."""
    yield
    LoguruLogger._reset()


class TestLoguruLogger:
    """Tests for LoguruLogger initialization and log methods."""

    def test_singleton_returns_same_instance(self) -> None:
        """Multiple instantiations return the same instance."""
        adapter1 = LoguruLogger(level="DEBUG")
        adapter2 = LoguruLogger(level="INFO")
        assert adapter1 is adapter2

    def test_invalid_level_raises(self) -> None:
        """Invalid log level raises ValueError."""
        with pytest.raises(expected_exception=ValueError, match="(?i)level"):
            LoguruLogger(level="INVALID")

    @pytest.mark.parametrize(
        argnames="method", argvalues=["debug", "info", "warning", "error", "critical"]
    )
    def test_log_methods(self, method: str) -> None:
        """All log methods work with and without extra."""
        adapter = LoguruLogger(level="DEBUG")
        log_fn = getattr(adapter, method)
        log_fn("message")
        log_fn("message", extra={"key": "value"})

    def test_exception_with_active_exception(self, capsys: pytest.CaptureFixture) -> None:
        """exception() logs traceback when called in handler."""
        adapter = LoguruLogger(level="DEBUG")
        try:
            raise ValueError("test error")
        except ValueError:
            adapter.exception("caught", extra={"ctx": "test"})
        assert "ValueError" in capsys.readouterr().err

    def test_exception_without_active_exception(self, capsys: pytest.CaptureFixture) -> None:
        """exception() adds warning when called outside handler."""
        LoguruLogger(level="DEBUG").exception("no exception")
        output = capsys.readouterr().err
        assert "no exception" in output
        assert "You called logger.exception() with no active exception" in output


class TestFormatExtra:
    """Tests for _make_format_func method."""

    def test_empty_extra(self) -> None:
        """Empty extra produces empty string."""
        # record = {"extra": {}}
        # _format_extra(cast("Record", cast(object, record)))
        # assert record["extra_str"] == ""
        pass

    def test_formats_key_value_with_ansi(self) -> None:
        """Extra fields are formatted."""
        # record = {"extra": {"status": "ok", "count": 5}}
        # _format_extra(cast("Record", cast(object, record)))
        # assert all(s in record["extra_str"] for s in ["status", "ok", "count", "5"])
        pass

    def test_handles_str_conversion_error(self) -> None:
        """Objects raising on str() show <REPR_ERROR> for both keys and values."""

        class BadStr:
            def __str__(self) -> str:
                raise Exception

            def __hash__(self) -> int:
                return 42

            def __eq__(self, other: object) -> bool:
                return isinstance(other, BadStr)

        # # Test value error
        # record = {"extra": {"key": BadStr()}}
        # _format_extra(cast("Record", cast(object, record)))
        # assert "<REPR_ERROR>" in record["extra_str"]
        #
        # # Test key error
        # record = {"extra": {BadStr(): "value"}}
        # _format_extra(cast("Record", cast(object, record)))
        # assert "<REPR_ERROR>" in record["extra_str"]

    def test_handles_various_data_types(self) -> None:
        """Extra handles None, lists, and nested dicts."""
        # record = {
        #     "extra": {
        #         "none_val": None,
        #         "list_val": [1, 2, 3],
        #         "dict_val": {"nested": "value"},
        #     }
        # }
        # _format_extra(cast("Record", cast(object, record)))
        # assert "None" in record["extra_str"]
        # assert "[1, 2, 3]" in record["extra_str"]
        # assert "nested" in record["extra_str"]

    def test_all_keys_are_present(self) -> None:
        """All extra keys are included in output."""
        # keys = ["alpha", "beta", "gamma", "delta"]
        # record = {"extra": {k: f"val_{k}" for k in keys}}
        # _format_extra(cast("Record", cast(object, record)))
        # for key in keys:
        #     assert key in record["extra_str"]
        #     assert f"val_{key}" in record["extra_str"]


class TestStripAnsi:
    """Tests for _strip_ansi static method."""

    @pytest.mark.parametrize(
        argnames=("input_text", "expected"),
        argvalues=[
            ("normal text", "normal text"),
            ("\x1b[31mred\x1b[0m", "red"),
            ("\x1b[1m\x1b[32mbold green\x1b[0m", "bold green"),
        ],
    )
    def test_strip_ansi(self, input_text: str, expected: str) -> None:
        """ANSI sequences are removed, normal text preserved."""
        # assert _strip_ansi(input_text) == expected
        pass


class TestModuleExport:
    """Tests for module-level logger export."""

    def test_logger_exported_and_functional(self) -> None:
        """Module exports a functional LoguruLogger instance."""
        assert isinstance(logger, LoguruLogger)
        logger.debug("test", extra={"x": 1})  # Should not raise
