from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest

from bioetl.core.validators import (
    JSON_PRIMITIVE_TYPES,
    assert_iterable,
    assert_json_mapping,
    assert_list_of,
    is_iterable,
    is_json_mapping,
    is_list_of,
)


class IndexOnlySequence:
    def __init__(self, *values: Any) -> None:
        self._values = list(values)

    def __getitem__(self, index: int) -> Any:
        if index >= len(self._values):
            raise IndexError
        return self._values[index]


class NonIterable:
    pass


def test_is_iterable_excludes_strings_by_default() -> None:
    assert not is_iterable("text")
    assert not is_iterable(b"bytes")


def test_is_iterable_accepts_strings_when_requested() -> None:
    assert is_iterable("text", exclude_str=False)
    assert is_iterable(b"bytes", exclude_str=False)


def test_is_iterable_handles_index_only_sequence() -> None:
    sequence = IndexOnlySequence(1, 2, 3)
    assert is_iterable(sequence)
    assert_iterable(sequence, argument_name="sequence")


def test_is_iterable_rejects_non_iterable() -> None:
    candidate = NonIterable()
    assert not is_iterable(candidate)
    with pytest.raises(TypeError, match="non_iterable"):
        assert_iterable(candidate, argument_name="non_iterable")


def test_is_list_of_checks_predicate() -> None:
    assert is_list_of([1, 2, 3], lambda item: isinstance(item, int))
    assert not is_list_of([1, "2", 3], lambda item: isinstance(item, int))
    assert not is_list_of((1, 2), lambda item: isinstance(item, int))


def test_assert_list_of_reports_invalid_items() -> None:
    with pytest.raises(TypeError, match="numbers"):
        assert_list_of("123", lambda _: True, argument_name="numbers")

    with pytest.raises(ValueError, match="indices \\[1, 3\\]"):
        assert_list_of(
            [1, "two", 3, None],
            lambda item: isinstance(item, int),
            argument_name="numbers",
            predicate_name="isinstance(item, int)",
        )


def test_is_json_mapping_accepts_nested_structures() -> None:
    payload: Mapping[str, Any] = {
        "name": "example",
        "count": 2,
        "flags": [True, False],
        "details": {"meta": None, "values": [1, 2.5]},
    }
    assert is_json_mapping(payload)
    assert_json_mapping(payload)


def test_is_json_mapping_rejects_non_string_keys() -> None:
    payload = {1: "value"}
    assert not is_json_mapping(payload)
    with pytest.raises(TypeError):
        assert_json_mapping(payload)


def test_is_json_mapping_rejects_unsupported_values() -> None:
    payload = {"data": {1, 2, 3}}
    assert not is_json_mapping(payload)
    with pytest.raises(TypeError):
        assert_json_mapping(payload)


def test_is_json_mapping_detects_cycles() -> None:
    payload: dict[str, Any] = {"self": {}}
    payload["self"] = payload
    assert not is_json_mapping(payload)
    with pytest.raises(TypeError):
        assert_json_mapping(payload)


def test_json_primitive_types_are_subset_of_supported_types() -> None:
    assert JSON_PRIMITIVE_TYPES == (str, int, float, bool, type(None))

