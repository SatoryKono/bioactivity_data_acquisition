"""Unit tests for mapping utility helpers."""

from __future__ import annotations

import pytest

from bioetl.core.mapping_utils import stringify_mapping


@pytest.mark.unit
def test_stringify_mapping_handles_various_key_types() -> None:
    mapping = {
        "string": "value",
        42: "int",
        3.14: "float",
        False: "bool",
        None: "none",
        ("tuple", 1): "tuple",
    }

    result = stringify_mapping(mapping)

    assert result == {
        "string": "value",
        "42": "int",
        "3.14": "float",
        "False": "bool",
        "None": "none",
        "('tuple', 1)": "tuple",
    }
