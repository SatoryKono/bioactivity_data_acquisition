"""Unit tests for testitem array serialization and flattening."""

from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest

from bioetl.pipelines.testitem.testitem_transform import (
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

    def test_molecule_synonyms_with_nested_synonyms(self) -> None:
        """Test serialization of molecule_synonyms with nested synonyms array."""
        items = [
            {
                "molecule_synonym": "TAMIFLU",
                "syn_type": "TRADE_NAME",
                "synonyms": ["RO-64-0796", "GS-4104"],
            },
        ]
        result = serialize_objects(items)
        # Verify that synonyms field is present and contains JSON-serialized array
        assert "molecule_synonym" in result
        assert "syn_type" in result
        assert "synonyms" in result
        assert "TAMIFLU" in result
        assert "TRADE_NAME" in result
        # Nested array should be JSON-serialized
        assert "RO-64-0796" in result
        assert "GS-4104" in result
        # Verify JSON format is used (contains brackets)
        assert "[" in result and "]" in result

    def test_molecule_synonyms_multiple_entries(self) -> None:
        """Test serialization of multiple molecule_synonyms entries."""
        items: list[dict[str, str | list[str]]] = [
            {
                "molecule_synonym": "TAMIFLU",
                "syn_type": "TRADE_NAME",
                "synonyms": ["RO-64-0796"],
            },
            {
                "molecule_synonym": "Oseltamivir",
                "syn_type": "SYSTEMATIC",
                "synonyms": [],
            },
        ]
        result = serialize_objects(items)
        assert "molecule_synonym" in result
        assert "syn_type" in result
        assert "synonyms" in result
        assert "TAMIFLU" in result
        assert "Oseltamivir" in result
        # Verify header/rows format
        assert "/" in result
        assert "|" in result

    def test_escaping(self) -> None:
        """Test escaping of pipe and slash characters."""
        items = [{"a": "A|B", "b": "C/D"}]
        result = serialize_objects(items)
        assert "\\|" in result or "\\/" in result
        assert "A|B" not in result or "C/D" not in result

    def test_escaping_pipe_in_values(self) -> None:
        """Test escaping of pipe character in values."""
        items = [{"xref_id": "X1|X2", "xref_name": "Name with|pipe"}]
        result = serialize_objects(items)
        # Verify pipe is escaped
        assert "\\|" in result
        # Verify original pipe is not present
        assert "X1|X2" not in result
        assert "Name with|pipe" not in result

    def test_escaping_slash_in_values(self) -> None:
        """Test escaping of slash character in values."""
        items = [{"xref_id": "X1/X2", "xref_name": "Name/with/slash"}]
        result = serialize_objects(items)
        # Verify slash is escaped
        assert "\\/" in result
        # Verify original slash is not present in unescaped form
        assert "X1/X2" not in result or "\\/" in result

    def test_escaping_backslash(self) -> None:
        """Test escaping of backslash character."""
        items = [{"xref_id": "X1\\X2", "xref_name": "Name\\with\\backslash"}]
        result = serialize_objects(items)
        # Verify backslash is escaped (becomes \\\\)
        assert "\\\\" in result

    def test_escaping_all_delimiters(self) -> None:
        """Test escaping of all delimiter characters (|, /, \\)."""
        items = [{"a": "Value|with/backslash" + chr(92)}]  # chr(92) is backslash
        result = serialize_objects(items)
        # Verify all delimiters are escaped
        assert "\\|" in result
        assert "\\/" in result
        assert "\\\\" in result

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

    def test_flatten_molecule_properties_all_fields(self) -> None:
        """Test flattening of all molecule_properties fields from requirements."""
        df = pd.DataFrame({
            "molecule_chembl_id": ["CHEMBL1"],
            "molecule_properties": [
                {
                    "alogp": 2.5,
                    "aromatic_rings": 1,
                    "cx_logd": 1.5,
                    "cx_logp": 2.3,
                    "cx_most_apka": 10.2,
                    "cx_most_bpka": 3.5,
                    "full_molformula": "C2H6O",
                    "full_mwt": 46.07,
                    "hba": 1,
                    "hba_lipinski": 1,
                    "hbd": 1,
                    "hbd_lipinski": 1,
                    "heavy_atoms": 3,
                    "molecular_species": "NEUTRAL",
                    "mw_freebase": 46.07,
                    "mw_monoisotopic": 46.0419,
                    "num_lipinski_ro5_violations": 0,
                    "num_ro5_violations": 0,
                    "psa": 20.23,
                    "qed_weighted": 0.85,
                    "ro3_pass": 1,
                    "rtb": 1,
                },
            ],
        })
        all_fields = [
            "alogp",
            "aromatic_rings",
            "cx_logd",
            "cx_logp",
            "cx_most_apka",
            "cx_most_bpka",
            "full_molformula",
            "full_mwt",
            "hba",
            "hba_lipinski",
            "hbd",
            "hbd_lipinski",
            "heavy_atoms",
            "molecular_species",
            "mw_freebase",
            "mw_monoisotopic",
            "num_lipinski_ro5_violations",
            "num_ro5_violations",
            "psa",
            "qed_weighted",
            "ro3_pass",
            "rtb",
        ]
        result = flatten_object_col(df, "molecule_properties", all_fields, "molecule_properties__")
        # Verify all fields are flattened
        for field in all_fields:
            assert f"molecule_properties__{field}" in result.columns
        # Verify values are preserved
        assert result["molecule_properties__alogp"].iloc[0] == 2.5
        assert result["molecule_properties__aromatic_rings"].iloc[0] == 1
        assert result["molecule_properties__full_molformula"].iloc[0] == "C2H6O"
        assert result["molecule_properties__molecular_species"].iloc[0] == "NEUTRAL"
        assert result["molecule_properties__ro3_pass"].iloc[0] == 1

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

    def test_flatten_molecule_structures_all_fields(self) -> None:
        """Test flattening of all molecule_structures fields including molfile and standard_inchi."""
        df = pd.DataFrame({
            "molecule_chembl_id": ["CHEMBL1"],
            "molecule_structures": [
                {
                    "canonical_smiles": "CCO",
                    "molfile": "\n  Mrv2014 01012024\n\n  3  2  0  0  0  0  0  0  0  0999 V2000\n",
                    "standard_inchi": "InChI=1S/C2H6O/c1-2-3/h3H,2H2,1H3",
                    "standard_inchi_key": "LFQSCWFLJHTTHZ-UHFFFAOYSA-N",
                },
            ],
        })
        all_fields = ["canonical_smiles", "molfile", "standard_inchi", "standard_inchi_key"]
        result = flatten_object_col(df, "molecule_structures", all_fields, "molecule_structures__")
        # Verify all fields are flattened
        for field in all_fields:
            assert f"molecule_structures__{field}" in result.columns
        # Verify values are preserved
        assert result["molecule_structures__canonical_smiles"].iloc[0] == "CCO"
        assert "Mrv2014" in result["molecule_structures__molfile"].iloc[0]
        assert "InChI=1S" in result["molecule_structures__standard_inchi"].iloc[0]
        assert result["molecule_structures__standard_inchi_key"].iloc[0] == "LFQSCWFLJHTTHZ-UHFFFAOYSA-N"

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

    def test_flatten_with_empty_dict(self) -> None:
        """Test flattening when nested object is empty dict."""
        df = pd.DataFrame({
            "molecule_chembl_id": ["CHEMBL1"],
            "molecule_properties": [{}],
        })
        result = flatten_object_col(
            df,
            "molecule_properties",
            ["full_mwt", "alogp"],
            "molecule_properties__",
        )
        assert "molecule_properties__full_mwt" in result.columns
        assert "molecule_properties__alogp" in result.columns
        assert pd.isna(result["molecule_properties__full_mwt"].iloc[0])
        assert pd.isna(result["molecule_properties__alogp"].iloc[0])


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

    def test_transform_with_molecule_synonyms(self) -> None:
        """Test transform with molecule_synonyms serialization."""
        df = pd.DataFrame({
            "molecule_chembl_id": ["CHEMBL1"],
            "molecule_synonyms": [
                {
                    "molecule_synonym": "TAMIFLU",
                    "syn_type": "TRADE_NAME",
                    "synonyms": ["RO-64-0796"],
                },
            ],
        })

        config = MagicMock()
        config.transform = MagicMock()
        config.transform.enable_flatten = True
        config.transform.enable_serialization = True
        config.transform.flatten_objects = {}
        config.transform.arrays_simple_to_pipe = []
        config.transform.arrays_objects_to_header_rows = ["molecule_synonyms"]

        result = transform(df, config)

        # Check serialization
        assert "molecule_synonyms__flat" in result.columns
        assert "molecule_synonyms" not in result.columns
        # Verify serialized format contains key fields
        serialized = result["molecule_synonyms__flat"].iloc[0]
        assert "molecule_synonym" in serialized
        assert "syn_type" in serialized
        assert "TAMIFLU" in serialized

    def test_transform_empty_arrays(self) -> None:
        """Test transform with empty arrays."""
        df = pd.DataFrame({
            "molecule_chembl_id": ["CHEMBL1"],
            "atc_classifications": [[]],
            "cross_references": [[]],
            "molecule_synonyms": [[]],
        })

        config = MagicMock()
        config.transform = MagicMock()
        config.transform.enable_flatten = True
        config.transform.enable_serialization = True
        config.transform.flatten_objects = {}
        config.transform.arrays_simple_to_pipe = ["atc_classifications"]
        config.transform.arrays_objects_to_header_rows = ["cross_references", "molecule_synonyms"]

        result = transform(df, config)

        # Empty arrays should serialize to empty strings
        assert result["atc_classifications"].iloc[0] == ""
        assert result["cross_references__flat"].iloc[0] == ""
        assert result["molecule_synonyms__flat"].iloc[0] == ""

    def test_transform_none_values(self) -> None:
        """Test transform with None values in arrays."""
        df = pd.DataFrame({
            "molecule_chembl_id": ["CHEMBL1"],
            "atc_classifications": [None],
            "cross_references": [None],
            "molecule_synonyms": [None],
        })

        config = MagicMock()
        config.transform = MagicMock()
        config.transform.enable_flatten = True
        config.transform.enable_serialization = True
        config.transform.flatten_objects = {}
        config.transform.arrays_simple_to_pipe = ["atc_classifications"]
        config.transform.arrays_objects_to_header_rows = ["cross_references", "molecule_synonyms"]

        result = transform(df, config)

        # None values should serialize to empty strings
        assert result["atc_classifications"].iloc[0] == ""
        assert result["cross_references__flat"].iloc[0] == ""
        assert result["molecule_synonyms__flat"].iloc[0] == ""

    def test_transform_missing_columns(self) -> None:
        """Test transform when columns are missing from DataFrame."""
        df = pd.DataFrame({
            "molecule_chembl_id": ["CHEMBL1"],
        })

        config = MagicMock()
        config.transform = MagicMock()
        config.transform.enable_flatten = True
        config.transform.enable_serialization = True
        config.transform.flatten_objects = {
            "molecule_hierarchy": ["molecule_chembl_id", "parent_chembl_id"],
        }
        config.transform.arrays_simple_to_pipe = ["atc_classifications"]
        config.transform.arrays_objects_to_header_rows = ["cross_references"]

        result = transform(df, config)

        # Missing columns should be handled gracefully
        # Flattening should create columns with None values
        assert "molecule_hierarchy__molecule_chembl_id" in result.columns
        assert "molecule_hierarchy__parent_chembl_id" in result.columns
        # Serialization should skip missing columns (no error)
        assert "atc_classifications" not in result.columns or result["atc_classifications"].isna().all()

    def test_serialize_invalid_types(self) -> None:
        """Test serialization with invalid types (non-list, non-dict)."""
        # Test serialize_simple_list with invalid types
        result1 = serialize_simple_list(123)
        assert result1.endswith("|")
        assert "123" in result1

        result2 = serialize_simple_list({"key": "value"})
        assert result2.endswith("|")

        # Test serialize_objects with invalid types
        result3 = serialize_objects("not_a_list")
        assert result3 != ""

        result4 = serialize_objects(123)
        assert result4 != ""

    def test_flatten_invalid_types(self) -> None:
        """Test flattening with invalid nested object types."""
        df = pd.DataFrame({
            "molecule_chembl_id": ["CHEMBL1"],
            "molecule_properties": ["not_a_dict"],  # String instead of dict
        })
        result = flatten_object_col(
            df,
            "molecule_properties",
            ["full_mwt", "alogp"],
            "molecule_properties__",
        )
        # Should handle gracefully, creating columns with None values
        assert "molecule_properties__full_mwt" in result.columns
        assert "molecule_properties__alogp" in result.columns
        assert pd.isna(result["molecule_properties__full_mwt"].iloc[0])
        assert pd.isna(result["molecule_properties__alogp"].iloc[0])

    def test_serialize_list_with_mixed_types(self) -> None:
        """Test serialization of list with mixed types."""
        result = serialize_simple_list(["A01AA", 123, None, "A01AB"])
        assert "A01AA" in result
        assert "123" in result
        assert "A01AB" in result
        assert result.endswith("|")

    def test_serialize_objects_with_mixed_object_types(self) -> None:
        """Test serialization of array with mixed object types."""
        items = [
            {"xref_id": "X1", "xref_name": "N1"},
            "not_a_dict",  # Invalid type
            {"xref_id": "X2"},
        ]
        result = serialize_objects(items)
        # Should handle gracefully, serializing non-dict items as JSON
        assert "xref_id" in result
        assert "X1" in result
        assert "X2" in result

    def test_flatten_with_partial_fields(self) -> None:
        """Test flattening when nested object has only some fields."""
        df = pd.DataFrame({
            "molecule_chembl_id": ["CHEMBL1"],
            "molecule_properties": [
                {
                    "full_mwt": 100.0,
                    # alogp is missing
                },
            ],
        })
        result = flatten_object_col(
            df,
            "molecule_properties",
            ["full_mwt", "alogp"],
            "molecule_properties__",
        )
        assert result["molecule_properties__full_mwt"].iloc[0] == 100.0
        assert pd.isna(result["molecule_properties__alogp"].iloc[0])

    def test_serialize_empty_string_in_list(self) -> None:
        """Test serialization of list with empty strings."""
        result = serialize_simple_list(["A01AA", "", "A01AB"])
        assert "A01AA" in result
        assert "A01AB" in result
        assert result.endswith("|")

    def test_serialize_objects_with_empty_string_values(self) -> None:
        """Test serialization of objects with empty string values."""
        items = [{"xref_id": "X1", "xref_name": "", "xref_src": "SRC"}]
        result = serialize_objects(items)
        assert "xref_id" in result
        assert "X1" in result
        assert "SRC" in result
        # Empty string should be preserved
        assert "/X1|" in result or "|X1|" in result

