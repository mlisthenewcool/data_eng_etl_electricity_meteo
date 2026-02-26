"""Unit tests for the core logging module."""

from enum import Enum, StrEnum
from pathlib import Path
from typing import Any

import pytest

from data_eng_etl_electricity_meteo.core.logger import (
    _CYAN,
    _DIM,
    _RESET,
    _STRUCTLOG_INTERNAL_KEYS,
    _build_level_styles,
    _colorize_event,
    _flatten_and_normalize,
    _flatten_dict,
    _normalize_value,
    _prepend_logger_name,
    _visual_len,
    _walk,
)

# Sentinel values for unused structlog processor parameters.
_LOGGER = None
_METHOD = "info"


# ---------------------------------------------------------------------------
# _normalize_value
# ---------------------------------------------------------------------------


class TestNormalizeValue:
    # fmt: off
    @pytest.mark.parametrize(
        argnames="value, expected",
        argvalues=[
            pytest.param(True,             "True",     id="bool_true_returns_string"),
            pytest.param(False,            "False",    id="bool_false_returns_string"),
            pytest.param("hello",          "hello",    id="str_passthrough"),
            pytest.param(42,               42,         id="int_passthrough"),
            pytest.param(3.14,             3.14,       id="float_passthrough"),
            pytest.param(Path("/tmp/foo"), "/tmp/foo", id="path_becomes_string"),
        ],
    )
    # fmt: on
    def test_known_types(self, value: object, expected: str | int | float) -> None:
        assert _normalize_value(value) == expected

    def test_str_enum_treated_as_str(self) -> None:
        # StrEnum is also a str subclass — takes the str branch, not the Enum branch.
        class Status(StrEnum):
            ACTIVE = "active"

        assert _normalize_value(Status.ACTIVE) == "active"

    def test_enum_unwraps_value(self) -> None:
        # Plain Enum (not a str/int subclass) takes the Enum branch and unwraps .value.
        class Color(Enum):
            RED = "red"

        assert _normalize_value(Color.RED) == "red"

    def test_unknown_type_returns_repr(self) -> None:
        obj = object()
        result = _normalize_value(obj)
        assert isinstance(result, str)
        assert repr(obj) in result


# ---------------------------------------------------------------------------
# _flatten_dict
# ---------------------------------------------------------------------------


# fmt: off
@pytest.mark.parametrize(
    argnames="prefix, mapping, expected",
    argvalues=[
        pytest.param("x", {"a": 1},        {"x.a": 1},   id="flat_dict"),
        pytest.param("x", {"a": {"b": 2}}, {"x.a.b": 2}, id="nested_dict"),
        pytest.param("x", {"a": None},     {},           id="none_leaf_is_dropped"),
        pytest.param("",  {"a": 1},        {"a": 1},     id="empty_prefix_uses_key_as_is"),
    ],
)
# fmt: on
def test_flatten_dict(prefix: str, mapping: dict[str, Any], expected: dict[str, Any]) -> None:
    assert _flatten_dict(prefix, mapping) == expected


# ---------------------------------------------------------------------------
# _flatten_and_normalize
# ---------------------------------------------------------------------------


class TestFlattenAndNormalize:
    def test_internal_keys_are_preserved(self) -> None:
        event_dict = {key: f"val_{key}" for key in _STRUCTLOG_INTERNAL_KEYS}
        result = _flatten_and_normalize(_LOGGER, _METHOD, event_dict)
        assert result == event_dict

    def test_external_keys_are_flattened_and_normalized(self) -> None:
        event_dict: dict[str, Any] = {"event": "hello", "src": {"provider": "IGN"}, "flag": True}
        result = _flatten_and_normalize(_LOGGER, _METHOD, event_dict)
        assert result["src.provider"] == "IGN"
        assert result["flag"] == "True"

    def test_internal_none_is_preserved(self) -> None:
        # Internal keys bypass normalization — even if None.
        event_dict: dict[str, Any] = {"event": "hello", "level": None}
        result = _flatten_and_normalize(_LOGGER, _METHOD, event_dict)
        assert result["level"] is None


# ---------------------------------------------------------------------------
# _visual_len
# ---------------------------------------------------------------------------

# fmt: off
@pytest.mark.parametrize(
    argnames="s, expected",
    argvalues=[
        pytest.param("hello",                5, id="plain_string"),
        pytest.param("\033[32mhello\033[0m", 5, id="ansi_colored_string"),
        pytest.param("",                     0, id="empty_string"),
        pytest.param("\033[1m\033[0m",       0, id="only_ansi_codes"),
    ],
)
# fmt: on
def test_visual_len(s: str, expected: int) -> None:
    assert _visual_len(s) == expected


# ---------------------------------------------------------------------------
# _build_level_styles
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    argnames="level, expected",
    argvalues=[
        pytest.param("debug", _DIM, id="debug_is_dim"),
        pytest.param("info", "", id="info_is_uncolored"),
    ],
)
def test_level_style(level: str, expected: str) -> None:
    assert _build_level_styles()[level] == expected


# ---------------------------------------------------------------------------
# _colorize_event
# ---------------------------------------------------------------------------


class TestColorizeEvent:
    def test_known_level_wraps_event_with_ansi(self) -> None:
        level = "warning"
        styles = _build_level_styles()
        processor = _colorize_event(styles)
        event_dict = {"level": level, "event": "hello"}
        processor(_LOGGER, _METHOD, event_dict)
        assert event_dict["event"].startswith(styles[level])
        assert event_dict["event"].endswith(_RESET)
        assert "hello" in event_dict["event"]

    def test_info_level_leaves_event_unchanged(self) -> None:
        # "info" maps to "" (empty string) — falsy, no color applied.
        styles = _build_level_styles()
        processor = _colorize_event(styles)
        event_dict = {"level": "info", "event": "hello"}
        processor(_LOGGER, _METHOD, event_dict)
        assert event_dict["event"] == "hello"


# ---------------------------------------------------------------------------
# _prepend_logger_name
# ---------------------------------------------------------------------------


class TestPrependLoggerName:
    def test_with_name_no_colors(self) -> None:
        name, msg = "myapp", "msg"
        processor = _prepend_logger_name(use_colors=False)
        event_dict = {"logger_name": name, "event": msg}
        processor(_LOGGER, _METHOD, event_dict)
        assert event_dict["event"] == f"[{name}] {msg}"
        assert "logger_name" not in event_dict

    def test_without_name(self) -> None:
        processor = _prepend_logger_name(use_colors=False)
        event_dict = {"event": "msg"}
        processor(_LOGGER, _METHOD, event_dict)
        assert event_dict["event"] == "msg"

    def test_with_colors_wraps_name_in_cyan(self) -> None:
        name = "myapp"
        processor = _prepend_logger_name(use_colors=True)
        event_dict = {"logger_name": name, "event": "msg"}
        processor(_LOGGER, _METHOD, event_dict)
        assert name in event_dict["event"]
        assert _CYAN in event_dict["event"]

    def test_padding_pads_short_string(self) -> None:
        processor = _prepend_logger_name(use_colors=False, pad_to=20)
        event_dict = {"logger_name": "a", "event": "b"}
        processor(_LOGGER, _METHOD, event_dict)
        # "[a] b" = 5 chars, padded to 20
        assert len(event_dict["event"]) == 20

    def test_padding_does_not_truncate_long_string(self) -> None:
        name, msg = "long_name", "long_event"
        processor = _prepend_logger_name(use_colors=False, pad_to=5)
        event_dict = {"logger_name": name, "event": msg}
        processor(_LOGGER, _METHOD, event_dict)
        assert event_dict["event"] == f"[{name}] {msg}"


# ---------------------------------------------------------------------------
# _walk
# ---------------------------------------------------------------------------


class TestWalk:
    @pytest.mark.parametrize(
        argnames="value, expected",
        argvalues=[
            pytest.param(None, None, id="none_is_preserved"),
            pytest.param({"a": None}, {"a": None}, id="nested_none_in_dict_is_preserved"),
            pytest.param([1, None, "x"], [1, None, "x"], id="list_preserved"),
            pytest.param((1, 2, 3), [1, 2, 3], id="tuple_converted_to_list"),
        ],
    )
    def test_simple_structures(self, value: object, expected: object) -> None:
        assert _walk(value) == expected

    def test_deeply_nested(self) -> None:
        assert _walk({"a": {"b": [None, True, Path("/x")]}}) == {"a": {"b": [None, "True", "/x"]}}
