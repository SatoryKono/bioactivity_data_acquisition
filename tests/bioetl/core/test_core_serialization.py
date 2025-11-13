from __future__ import annotations

import pandas as pd

from bioetl.core import (
    escape_delims,
    header_rows_serialize,
    serialize_array_fields,
    serialize_objects,
    serialize_simple_list,
)


def test_escape_delims_handles_special_characters() -> None:
    assert escape_delims("A|B/C\\D") == "A\\|B\\/C\\\\D"


def test_header_rows_serialize_produces_deterministic_output() -> None:
    payload = [{"a": "A", "b": "B"}, {"b": "B2", "c": "C2"}]
    assert header_rows_serialize(payload) == "a|b|c/A|B|/|B2|C2"


def test_header_rows_serialize_handles_nested_values() -> None:
    payload = [{"a": [1, 2], "b": {"x": "y"}}]
    assert header_rows_serialize(payload) == 'a|b/[1, 2]|{"x": "y"}'


def test_serialize_array_fields_transforms_specified_columns() -> None:
    df = pd.DataFrame.from_records(
        [
            {"id": 1, "items": [{"a": "A"}]},
            {"id": 2, "items": []},
        ],
    )

    result = serialize_array_fields(df, ["items"])

    assert result.loc[0, "items"] == "a/A"
    assert result.loc[1, "items"] == ""


def test_serialize_simple_list_serializes_iterables() -> None:
    assert serialize_simple_list(["A", "B"]) == "A|B|"
    assert serialize_simple_list([]) == ""
    assert serialize_simple_list(None) == ""
    assert serialize_simple_list("Z") == "Z|"


def test_serialize_objects_delegates_to_header_serializer() -> None:
    payload = [{"x": "X"}]
    assert serialize_objects(payload) == "x/X"
    assert serialize_objects(None) == ""
