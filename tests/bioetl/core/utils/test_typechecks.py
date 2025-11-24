"""Tests for :mod:`bioetl.core.utils.typechecks`."""

from __future__ import annotations

from typing import Any

import pytest

from bioetl.core.utils.typechecks import is_dict, is_list


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ([1, 2, 3], True),
        ([], True),
        ((1, 2, 3), False),
        ("not a list", False),
        (None, False),
    ],
)
def test_is_list_runtime_behavior(value: Any, expected: bool) -> None:
    """``is_list`` should match ``list`` instances and reject other types."""

    assert is_list(value) is expected


def test_is_list_typeguard_narrowing() -> None:
    """``is_list`` should narrow the runtime type for type checkers."""

    candidate: Any = ["a", "b"]
    if is_list(candidate):
        candidate.append("c")
        assert candidate[-1] == "c"
    else:  # pragma: no cover - defensive branch to satisfy type checkers
        pytest.fail("Expected candidate to be treated as a list")


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ({"a": 1}, True),
        ({}, True),
        ([("a", 1)], False),
        ("not a dict", False),
        (None, False),
    ],
)
def test_is_dict_runtime_behavior(value: Any, expected: bool) -> None:
    """``is_dict`` should match ``dict`` instances and reject other types."""

    assert is_dict(value) is expected


def test_is_dict_typeguard_narrowing() -> None:
    """``is_dict`` should provide a typed mapping for type checkers."""

    candidate: Any = {"name": "value"}
    if is_dict(candidate):
        candidate["extra"] = 123
        assert set(candidate) == {"name", "extra"}
    else:  # pragma: no cover - defensive branch to satisfy type checkers
        pytest.fail("Expected candidate to be treated as a dict")
