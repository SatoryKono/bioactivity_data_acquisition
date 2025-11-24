from __future__ import annotations

from collections import UserDict

import pytest

from bioetl.schemas.metadata_utils import metadata_dict, normalize_sequence


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, ()),
        ("single", ("single",)),
        (b"bytes", ("b'bytes'",)),
        (("a", "b"), ("a", "b")),
        (["x", "y"], ("x", "y")),
        ((str(i) for i in range(2)), ("0", "1")),
    ],
)
def test_normalize_sequence(value: object, expected: tuple[str, ...]) -> None:
    assert normalize_sequence(value) == expected


def test_metadata_dict_merges_sources() -> None:
    base = {"version": "1.0", "column_order": ("a", "b")}
    overlay = UserDict({"description": "test"})

    result = metadata_dict(base, overlay, extra="value")

    assert result == {
        "version": "1.0",
        "column_order": ("a", "b"),
        "description": "test",
        "extra": "value",
    }
    assert base == {"version": "1.0", "column_order": ("a", "b")}
    assert overlay == {"description": "test"}
