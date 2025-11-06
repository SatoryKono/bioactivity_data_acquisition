"""Unit tests for target array serialization."""

from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest

from bioetl.pipelines.target.target_transform import (
    extract_and_serialize_component_synonyms,
    serialize_target_arrays,
)


@pytest.mark.unit
class TestExtractAndSerializeComponentSynonyms:
    """Test suite for extract_and_serialize_component_synonyms function."""

    def test_empty_list(self) -> None:
        """Test extraction from empty list."""
        result = extract_and_serialize_component_synonyms([])
        assert result == ""

    def test_none(self) -> None:
        """Test extraction from None."""
        result = extract_and_serialize_component_synonyms(None)
        assert result == ""

    def test_single_component_with_synonyms(self) -> None:
        """Test extraction from single component with synonyms."""
        components = [
            {
                "component_id": 1,
                "accession": "P12345",
                "target_component_synonyms": [
                    {"syn_type": "GENE_SYMBOL", "synonyms": ["EGFR", "ERBB1"]},
                    {"syn_type": "UNIPROT", "synonyms": "P00533"},
                ],
            }
        ]
        result = extract_and_serialize_component_synonyms(components)
        assert result != ""
        assert "syn_type" in result
        assert "synonyms" in result
        assert "GENE_SYMBOL" in result or "EGFR" in result

    def test_multiple_components_with_synonyms(self) -> None:
        """Test extraction from multiple components with synonyms."""
        components = [
            {
                "component_id": 1,
                "target_component_synonyms": [{"syn_type": "GENE_SYMBOL", "synonyms": ["EGFR"]}],
            },
            {
                "component_id": 2,
                "target_component_synonyms": [{"syn_type": "UNIPROT", "synonyms": "P00533"}],
            },
        ]
        result = extract_and_serialize_component_synonyms(components)
        assert result != ""
        assert "syn_type" in result
        assert "synonyms" in result

    def test_components_without_synonyms(self) -> None:
        """Test extraction from components without synonyms."""
        components = [
            {"component_id": 1, "accession": "P12345"},
            {"component_id": 2, "accession": "P67890"},
        ]
        result = extract_and_serialize_component_synonyms(components)
        assert result == ""

    def test_mixed_components(self) -> None:
        """Test extraction from mixed components (some with, some without synonyms)."""
        components = [
            {"component_id": 1, "accession": "P12345"},
            {
                "component_id": 2,
                "target_component_synonyms": [{"syn_type": "GENE_SYMBOL", "synonyms": ["EGFR"]}],
            },
        ]
        result = extract_and_serialize_component_synonyms(components)
        assert result != ""
        assert "syn_type" in result

    def test_synonyms_as_list_in_value(self) -> None:
        """Test extraction when synonyms field contains a list."""
        components = [
            {
                "component_id": 1,
                "target_component_synonyms": [
                    {"syn_type": "GENE_SYMBOL", "synonyms": ["EGFR", "ERBB1"]}
                ],
            }
        ]
        result = extract_and_serialize_component_synonyms(components)
        assert result != ""
        # synonyms should be JSON serialized since it's a list
        assert "synonyms" in result

    def test_single_dict_not_list(self) -> None:
        """Test extraction when input is a single dict (not a list)."""
        component = {
            "component_id": 1,
            "target_component_synonyms": [{"syn_type": "GENE_SYMBOL", "synonyms": ["EGFR"]}],
        }
        result = extract_and_serialize_component_synonyms(component)
        assert result != ""
        assert "syn_type" in result


@pytest.mark.unit
class TestSerializeTargetArrays:
    """Test suite for serialize_target_arrays function."""

    def test_cross_references_serialization(self) -> None:
        """Test serialization of cross_references."""
        df = pd.DataFrame(
            {
                "target_chembl_id": ["CHEMBL1"],
                "cross_references": [
                    [{"xref_id": "X1", "xref_name": "N1", "xref_src": "SRC"}]
                ],
            }
        )
        mock_config = MagicMock()
        mock_config.transform.arrays_to_header_rows = ["cross_references", "target_components"]

        result = serialize_target_arrays(df, mock_config)

        assert "cross_references__flat" in result.columns
        assert result["cross_references__flat"].iloc[0] != ""
        assert "xref_id" in result["cross_references__flat"].iloc[0]
        assert "X1" in result["cross_references__flat"].iloc[0]

    def test_target_components_serialization(self) -> None:
        """Test serialization of target_components."""
        df = pd.DataFrame(
            {
                "target_chembl_id": ["CHEMBL1"],
                "target_components": [
                    [
                        {
                            "component_id": 1,
                            "accession": "P12345",
                            "component_type": "PROTEIN",
                            "organism": "Homo sapiens",
                            "tax_id": 9606,
                        }
                    ]
                ],
            }
        )
        mock_config = MagicMock()
        mock_config.transform.arrays_to_header_rows = ["cross_references", "target_components"]

        result = serialize_target_arrays(df, mock_config)

        assert "target_components__flat" in result.columns
        assert result["target_components__flat"].iloc[0] != ""
        assert "component_id" in result["target_components__flat"].iloc[0]
        assert "accession" in result["target_components__flat"].iloc[0]

    def test_target_component_synonyms_extraction(self) -> None:
        """Test extraction and serialization of target_component_synonyms."""
        df = pd.DataFrame(
            {
                "target_chembl_id": ["CHEMBL1"],
                "target_components": [
                    [
                        {
                            "component_id": 1,
                            "target_component_synonyms": [
                                {"syn_type": "GENE_SYMBOL", "synonyms": ["EGFR", "ERBB1"]},
                                {"syn_type": "UNIPROT", "synonyms": "P00533"},
                            ],
                        }
                    ]
                ],
            }
        )
        mock_config = MagicMock()
        mock_config.transform.arrays_to_header_rows = ["cross_references", "target_components"]

        result = serialize_target_arrays(df, mock_config)

        assert "target_component_synonyms__flat" in result.columns
        assert result["target_component_synonyms__flat"].iloc[0] != ""
        assert "syn_type" in result["target_component_synonyms__flat"].iloc[0]
        assert "synonyms" in result["target_component_synonyms__flat"].iloc[0]

    def test_empty_arrays(self) -> None:
        """Test serialization of empty arrays."""
        df = pd.DataFrame(
            {
                "target_chembl_id": ["CHEMBL1"],
                "cross_references": [[]],
                "target_components": [[]],
            }
        )
        mock_config = MagicMock()
        mock_config.transform.arrays_to_header_rows = ["cross_references", "target_components"]

        result = serialize_target_arrays(df, mock_config)

        assert "cross_references__flat" in result.columns
        assert "target_components__flat" in result.columns
        assert "target_component_synonyms__flat" in result.columns
        assert result["cross_references__flat"].iloc[0] == ""
        assert result["target_components__flat"].iloc[0] == ""
        assert result["target_component_synonyms__flat"].iloc[0] == ""

    def test_none_values(self) -> None:
        """Test serialization of None values."""
        df = pd.DataFrame(
            {
                "target_chembl_id": ["CHEMBL1"],
                "cross_references": [None],
                "target_components": [None],
            }
        )
        mock_config = MagicMock()
        mock_config.transform.arrays_to_header_rows = ["cross_references", "target_components"]

        result = serialize_target_arrays(df, mock_config)

        assert "cross_references__flat" in result.columns
        assert "target_components__flat" in result.columns
        assert "target_component_synonyms__flat" in result.columns
        assert result["cross_references__flat"].iloc[0] == ""
        assert result["target_components__flat"].iloc[0] == ""
        assert result["target_component_synonyms__flat"].iloc[0] == ""

    def test_missing_columns(self) -> None:
        """Test serialization when columns are missing."""
        df = pd.DataFrame({"target_chembl_id": ["CHEMBL1"]})
        mock_config = MagicMock()
        mock_config.transform.arrays_to_header_rows = ["cross_references", "target_components"]

        result = serialize_target_arrays(df, mock_config)

        assert "cross_references__flat" in result.columns
        assert "target_components__flat" in result.columns
        assert "target_component_synonyms__flat" in result.columns
        assert result["cross_references__flat"].iloc[0] == ""
        assert result["target_components__flat"].iloc[0] == ""
        assert result["target_component_synonyms__flat"].iloc[0] == ""

    def test_escaping_delimiters(self) -> None:
        """Test escaping of pipe and slash delimiters."""
        df = pd.DataFrame(
            {
                "target_chembl_id": ["CHEMBL1"],
                "cross_references": [
                    [{"xref_id": "X1|X2", "xref_name": "N1/N2", "xref_src": "SRC\\1"}]
                ],
            }
        )
        mock_config = MagicMock()
        mock_config.transform.arrays_to_header_rows = ["cross_references", "target_components"]

        result = serialize_target_arrays(df, mock_config)

        flat = result["cross_references__flat"].iloc[0]
        # Check that delimiters are escaped
        assert "\\|" in flat or "\\/" in flat or "\\\\" in flat

    def test_removes_original_columns(self) -> None:
        """Test that original array columns are removed after serialization."""
        df = pd.DataFrame(
            {
                "target_chembl_id": ["CHEMBL1"],
                "cross_references": [[{"xref_id": "X1"}]],
                "target_components": [[{"component_id": 1}]],
            }
        )
        mock_config = MagicMock()
        mock_config.transform.arrays_to_header_rows = ["cross_references", "target_components"]

        result = serialize_target_arrays(df, mock_config)

        # Original columns should be removed
        assert "cross_references" not in result.columns
        assert "target_components" not in result.columns
        # Serialized columns should exist
        assert "cross_references__flat" in result.columns
        assert "target_components__flat" in result.columns

    def test_multiple_targets(self) -> None:
        """Test serialization with multiple targets."""
        df = pd.DataFrame(
            {
                "target_chembl_id": ["CHEMBL1", "CHEMBL2"],
                "cross_references": [
                    [{"xref_id": "X1", "xref_name": "N1"}],
                    [{"xref_id": "X2", "xref_name": "N2"}],
                ],
                "target_components": [
                    [{"component_id": 1}],
                    [{"component_id": 2}],
                ],
            }
        )
        mock_config = MagicMock()
        mock_config.transform.arrays_to_header_rows = ["cross_references", "target_components"]

        result = serialize_target_arrays(df, mock_config)

        assert len(result) == 2
        assert result["cross_references__flat"].iloc[0] != ""
        assert result["cross_references__flat"].iloc[1] != ""
        assert "X1" in result["cross_references__flat"].iloc[0]
        assert "X2" in result["cross_references__flat"].iloc[1]

