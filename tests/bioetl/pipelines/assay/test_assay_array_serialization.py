"""Unit tests for assay array serialization."""

from __future__ import annotations

import pandas as pd
import pytest

from bioetl.pipelines.assay.assay_transform import header_rows_serialize, serialize_array_fields


@pytest.mark.unit
class TestHeaderRowsSerialize:
    """Test suite for header_rows_serialize function."""

    def test_single_object(self) -> None:
        """Test serialization of single object."""
        items = [{"a": "A", "b": "B"}]
        result = header_rows_serialize(items)
        assert result == "a|b/A|B"

    def test_two_objects(self) -> None:
        """Test serialization of two objects."""
        items = [{"a": "A1", "b": "B1"}, {"a": "A2", "b": "B2"}]
        result = header_rows_serialize(items)
        assert result == "a|b/A1|B1/A2|B2"

    def test_missing_keys(self) -> None:
        """Test serialization with missing keys."""
        items = [{"a": "A"}]
        result = header_rows_serialize(items)
        assert result == "a/A"

    def test_missing_keys_multiple_objects(self) -> None:
        """Test serialization with missing keys in multiple objects."""
        items = [{"a": "A1"}, {"a": "A2", "b": "B2"}]
        result = header_rows_serialize(items)
        assert result == "a|b/A1|/A2|B2"

    def test_escaping_pipe_and_slash(self) -> None:
        """Test escaping of pipe and slash delimiters."""
        items = [{"x": "A|B", "y": "C/D"}]
        result = header_rows_serialize(items)
        assert result == "x|y/A\\|B|C\\/D"

    def test_escaping_backslash(self) -> None:
        """Test escaping of backslash."""
        items = [{"x": "A\\B"}]
        result = header_rows_serialize(items)
        assert result == "x/A\\\\B"

    def test_empty_list(self) -> None:
        """Test serialization of empty list."""
        items: list[dict[str, str]] = []
        result = header_rows_serialize(items)
        assert result == ""

    def test_none(self) -> None:
        """Test serialization of None."""
        result = header_rows_serialize(None)
        assert result == ""

    def test_non_list_value(self) -> None:
        """Test serialization of non-list value."""
        result = header_rows_serialize("string")
        # Should JSON serialize and escape
        assert result == '"string"'

    def test_nested_dict_value(self) -> None:
        """Test serialization with nested dict value."""
        items = [{"a": "A", "b": {"nested": "value"}}]
        result = header_rows_serialize(items)
        # Should JSON serialize nested dict and escape
        assert "a|b" in result
        assert "A" in result
        assert "nested" in result or '"nested"' in result

    def test_nested_list_value(self) -> None:
        """Test serialization with nested list value."""
        items = [{"a": "A", "b": [1, 2, 3]}]
        result = header_rows_serialize(items)
        # Should JSON serialize nested list and escape
        assert "a|b" in result
        assert "A" in result

    def test_mixed_key_order(self) -> None:
        """Test that key order is preserved from first item."""
        items = [{"z": "Z", "a": "A", "m": "M"}, {"a": "A2", "b": "B2"}]
        result = header_rows_serialize(items)
        # First item keys in order: z, a, m
        # Then b from second item (alphabetically)
        assert result == "z|a|m|b/Z|A|M|/|A2||B2"

    def test_key_order_deterministic(self) -> None:
        """Test that key order is deterministic."""
        items1 = [{"a": "A", "b": "B"}, {"c": "C"}]
        items2 = [{"a": "A", "b": "B"}, {"c": "C"}]
        result1 = header_rows_serialize(items1)
        result2 = header_rows_serialize(items2)
        assert result1 == result2
        assert result1 == "a|b|c/A|B|/||C"

    def test_none_values_in_dict(self) -> None:
        """Test serialization with None values in dict."""
        items = [{"a": "A", "b": None, "c": "C"}]
        result = header_rows_serialize(items)
        assert result == "a|b|c/A||C"

    def test_empty_string_values(self) -> None:
        """Test serialization with empty string values."""
        items = [{"a": "", "b": "B"}]
        result = header_rows_serialize(items)
        assert result == "a|b/|B"

    def test_numeric_values(self) -> None:
        """Test serialization with numeric values."""
        items = [{"a": 1, "b": 2.5}]
        result = header_rows_serialize(items)
        assert result == "a|b/1|2.5"

    def test_boolean_values(self) -> None:
        """Test serialization with boolean values."""
        items = [{"a": True, "b": False}]
        result = header_rows_serialize(items)
        assert result == "a|b/True|False"


@pytest.mark.unit
class TestSerializeArrayFields:
    """Test suite for serialize_array_fields function."""

    def test_serialize_single_column(self) -> None:
        """Test serialization of single column."""
        df = pd.DataFrame(
            {
                "id": [1, 2],
                "data": [[{"a": "A"}], [{"b": "B"}]],
            }
        )
        result = serialize_array_fields(df, ["data"])
        assert result["id"].tolist() == [1, 2]
        assert result["data"].iloc[0] == "a/A"
        assert result["data"].iloc[1] == "b/B"

    def test_serialize_multiple_columns(self) -> None:
        """Test serialization of multiple columns."""
        df = pd.DataFrame(
            {
                "id": [1],
                "classifications": [[{"type": "A"}]],
                "parameters": [[{"param": "value"}]],
            }
        )
        result = serialize_array_fields(df, ["classifications", "parameters"])
        assert (
            "type|/A" in result["classifications"].iloc[0]
            or result["classifications"].iloc[0] == "type/A"
        )
        assert (
            "param|/value" in result["parameters"].iloc[0]
            or result["parameters"].iloc[0] == "param/value"
        )

    def test_serialize_missing_column(self) -> None:
        """Test serialization when column is missing."""
        df = pd.DataFrame({"id": [1, 2]})
        result = serialize_array_fields(df, ["missing"])
        assert result.equals(df)

    def test_serialize_empty_dataframe(self) -> None:
        """Test serialization with empty dataframe."""
        df = pd.DataFrame({"id": [], "data": []})
        result = serialize_array_fields(df, ["data"])
        assert result.empty
        assert "data" in result.columns

    def test_serialize_none_values(self) -> None:
        """Test serialization with None values in column."""
        df = pd.DataFrame(
            {
                "id": [1, 2],
                "data": [None, [{"a": "A"}]],
            }
        )
        result = serialize_array_fields(df, ["data"])
        assert result["data"].iloc[0] == ""
        assert result["data"].iloc[1] == "a/A"

    def test_serialize_empty_lists(self) -> None:
        """Test serialization with empty lists."""
        df = pd.DataFrame(
            {
                "id": [1, 2],
                "data": [[], [{"a": "A"}]],
            }
        )
        result = serialize_array_fields(df, ["data"])
        assert result["data"].iloc[0] == ""
        assert result["data"].iloc[1] == "a/A"
