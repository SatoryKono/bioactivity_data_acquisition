"""Unit tests for testitem array serialization and flattening."""

from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest

from bioetl.pipelines.chembl.testitem_transform import (
    flatten_object_col,
    serialize_objects,
    serialize_simple_list,
    transform,
)


@pytest.mark.unit
class TestSerializeSimpleList:
    """Test suite for serialize_simple_list function."""

    def test_empty_list(self) -> None:
        """Test serialization of empty list."""
        result = serialize_simple_list([])
        assert result == ""

    def test_none(self) -> None:
        """Test serialization of None."""
        result = serialize_simple_list(None)
        assert result == ""

    def test_simple_list(self) -> None:
        """Test serialization of simple list."""
        result = serialize_simple_list(["A01AA", "A01AB"])
        assert result == "A01AA|A01AB|"

    def test_list_with_none(self) -> None:
        """Test serialization of list with None values."""
        result = serialize_simple_list(["A01AA", None, "A01AB"])
        assert result == "A01AA||A01AB|"

    def test_escaping(self) -> None:
        """Test escaping of pipe and slash characters."""
        result = serialize_simple_list(["A|B", "C/D"])
        assert result == "A\\|B|C\\/D|"

    def test_non_list_value(self) -> None:
        """Test serialization of non-list value."""
        result = serialize_simple_list("A01AA")
        assert result == "A01AA|"


@pytest.mark.unit
class TestSerializeObjects:
    """Test suite for serialize_objects function."""

    def test_empty_list(self) -> None:
        """Test serialization of empty list."""
        result = serialize_objects([])
        assert result == ""

    def test_none(self) -> None:
        """Test serialization of None."""
        result = serialize_objects(None)
        assert result == ""

    def test_single_object(self) -> None:
        """Test serialization of single object."""
        items = [{"xref_id": "X1", "xref_name": "N1", "xref_src": "SRC"}]
        result = serialize_objects(items)
        assert result == "xref_id|xref_name|xref_src/X1|N1|SRC"

    def test_multiple_objects(self) -> None:
        """Test serialization of multiple objects."""
        items = [
            {"xref_id": "X1", "xref_name": "N1"},
            {"xref_id": "X2", "xref_name": "N2", "xref_src": "SRC"},
        ]
        result = serialize_objects(items)
        assert result == "xref_id|xref_name|xref_src/X1|N1|/X2|N2|SRC"

    def test_nested_structures(self) -> None:
        """Test serialization of objects with nested lists/dicts."""
        items = [
            {
                "molecule_synonym": "TAMIFLU",
                "syn_type": "TRADE_NAME",
                "synonyms": ["RO-64-0796"],
            },
        ]
        result = serialize_objects(items)
        assert "synonyms" in result
        assert "RO-64-0796" in result
        # Nested list should be JSON-serialized
        assert "[" in result or "]" in result

    def test_escaping(self) -> None:
        """Test escaping of pipe and slash characters."""
        items = [{"a": "A|B", "b": "C/D"}]
        result = serialize_objects(items)
        assert "\\|" in result or "\\/" in result
        assert "A|B" not in result or "C/D" not in result

    def test_missing_fields(self) -> None:
        """Test serialization with missing fields."""
        items = [{"a": "A1"}, {"a": "A2", "b": "B2"}]
        result = serialize_objects(items)
        assert result == "a|b/A1|/A2|B2"


@pytest.mark.unit
class TestFlattenObjectCol:
    """Test suite for flatten_object_col function."""

    def test_flatten_molecule_hierarchy(self) -> None:
        """Test flattening of molecule_hierarchy."""
        df = pd.DataFrame({
            "molecule_chembl_id": ["CHEMBL1"],
            "molecule_hierarchy": [{"molecule_chembl_id": "CHEMBL1", "parent_chembl_id": "CHEMBL2"}],
        })
        result = flatten_object_col(df, "molecule_hierarchy", ["molecule_chembl_id", "parent_chembl_id"], "molecule_hierarchy__")
        assert "molecule_hierarchy__molecule_chembl_id" in result.columns
        assert "molecule_hierarchy__parent_chembl_id" in result.columns
        assert "molecule_hierarchy" not in result.columns
        assert result["molecule_hierarchy__molecule_chembl_id"].iloc[0] == "CHEMBL1"
        assert result["molecule_hierarchy__parent_chembl_id"].iloc[0] == "CHEMBL2"

    def test_flatten_molecule_properties(self) -> None:
        """Test flattening of molecule_properties."""
        df = pd.DataFrame({
            "molecule_chembl_id": ["CHEMBL1"],
            "molecule_properties": [
                {
                    "full_mwt": 100.0,
                    "alogp": 2.5,
                    "hbd": 2,
                    "hba": 3,
                },
            ],
        })
        result = flatten_object_col(
            df,
            "molecule_properties",
            ["full_mwt", "alogp", "hbd", "hba"],
            "molecule_properties__",
        )
        assert "molecule_properties__full_mwt" in result.columns
        assert "molecule_properties__alogp" in result.columns
        assert "molecule_properties__hbd" in result.columns
        assert "molecule_properties__hba" in result.columns
        assert result["molecule_properties__full_mwt"].iloc[0] == 100.0
        assert result["molecule_properties__alogp"].iloc[0] == 2.5

    def test_flatten_molecule_structures(self) -> None:
        """Test flattening of molecule_structures."""
        df = pd.DataFrame({
            "molecule_chembl_id": ["CHEMBL1"],
            "molecule_structures": [
                {
                    "canonical_smiles": "CCO",
                    "standard_inchi_key": "LFQSCWFLJHTTHZ-UHFFFAOYSA-N",
                },
            ],
        })
        result = flatten_object_col(
            df,
            "molecule_structures",
            ["canonical_smiles", "standard_inchi_key"],
            "molecule_structures__",
        )
        assert "molecule_structures__canonical_smiles" in result.columns
        assert "molecule_structures__standard_inchi_key" in result.columns
        assert result["molecule_structures__canonical_smiles"].iloc[0] == "CCO"

    def test_missing_column(self) -> None:
        """Test flattening when column doesn't exist."""
        df = pd.DataFrame({"molecule_chembl_id": ["CHEMBL1"]})
        result = flatten_object_col(df, "molecule_hierarchy", ["molecule_chembl_id", "parent_chembl_id"], "molecule_hierarchy__")
        assert "molecule_hierarchy__molecule_chembl_id" in result.columns
        assert "molecule_hierarchy__parent_chembl_id" in result.columns
        assert pd.isna(result["molecule_hierarchy__molecule_chembl_id"].iloc[0])

    def test_non_dict_value(self) -> None:
        """Test flattening when value is not a dict."""
        df = pd.DataFrame({
            "molecule_chembl_id": ["CHEMBL1"],
            "molecule_hierarchy": [None],
        })
        result = flatten_object_col(df, "molecule_hierarchy", ["molecule_chembl_id", "parent_chembl_id"], "molecule_hierarchy__")
        assert "molecule_hierarchy__molecule_chembl_id" in result.columns
        assert pd.isna(result["molecule_hierarchy__molecule_chembl_id"].iloc[0])


@pytest.mark.unit
class TestTransform:
    """Test suite for transform function."""

    def test_transform_with_flatten_and_serialize(self) -> None:
        """Test transform with flattening and serialization enabled."""
        df = pd.DataFrame({
            "molecule_chembl_id": ["CHEMBL1"],
            "molecule_hierarchy": [{"molecule_chembl_id": "CHEMBL1", "parent_chembl_id": "CHEMBL2"}],
            "molecule_properties": [{"full_mwt": 100.0, "alogp": 2.5}],
            "molecule_structures": [{"canonical_smiles": "CCO"}],
            "atc_classifications": [["A01AA", "A01AB"]],
            "cross_references": [{"xref_id": "X1", "xref_name": "N1"}],
        })

        config = MagicMock()
        config.transform = MagicMock()
        config.transform.enable_flatten = True
        config.transform.enable_serialization = True
        config.transform.flatten_objects = {
            "molecule_hierarchy": ["molecule_chembl_id", "parent_chembl_id"],
            "molecule_properties": ["full_mwt", "alogp"],
            "molecule_structures": ["canonical_smiles"],
        }
        config.transform.arrays_simple_to_pipe = ["atc_classifications"]
        config.transform.arrays_objects_to_header_rows = ["cross_references"]

        result = transform(df, config)

        # Check flattening
        assert "molecule_hierarchy__molecule_chembl_id" in result.columns
        assert "molecule_properties__full_mwt" in result.columns
        assert "molecule_structures__canonical_smiles" in result.columns

        # Check serialization
        assert result["atc_classifications"].iloc[0] == "A01AA|A01AB|"
        assert "cross_references__flat" in result.columns
        assert "cross_references" not in result.columns

    def test_transform_with_flatten_disabled(self) -> None:
        """Test transform with flattening disabled."""
        df = pd.DataFrame({
            "molecule_chembl_id": ["CHEMBL1"],
            "molecule_hierarchy": [{"molecule_chembl_id": "CHEMBL1"}],
        })

        config = MagicMock()
        config.transform = MagicMock()
        config.transform.enable_flatten = False
        config.transform.enable_serialization = True
        config.transform.flatten_objects = {}
        config.transform.arrays_simple_to_pipe = []
        config.transform.arrays_objects_to_header_rows = []

        result = transform(df, config)

        # Flattening should not happen
        assert "molecule_hierarchy__molecule_chembl_id" not in result.columns
        assert "molecule_hierarchy" in result.columns

    def test_transform_with_serialization_disabled(self) -> None:
        """Test transform with serialization disabled."""
        df = pd.DataFrame({
            "molecule_chembl_id": ["CHEMBL1"],
            "atc_classifications": [["A01AA", "A01AB"]],
        })

        config = MagicMock()
        config.transform = MagicMock()
        config.transform.enable_flatten = True
        config.transform.enable_serialization = False
        config.transform.flatten_objects = {}
        config.transform.arrays_simple_to_pipe = ["atc_classifications"]
        config.transform.arrays_objects_to_header_rows = []

        result = transform(df, config)

        # Serialization should not happen
        assert result["atc_classifications"].iloc[0] == ["A01AA", "A01AB"]

