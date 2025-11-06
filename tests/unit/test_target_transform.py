"""Unit tests for target array serialization."""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest

from bioetl.pipelines.target.target_transform import (
    extract_and_serialize_component_synonyms,
    flatten_target_components,
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


@pytest.mark.unit
class TestFlattenTargetComponents:
    """Test suite for flatten_target_components function."""

    def test_flatten_with_accessions(self) -> None:
        """Test flattening with UniProt accessions."""
        rec = {
            "target_chembl_id": "CHEMBL203",
            "target_components": [
                {"component_id": 1, "accession": "P00533"},
                {"component_id": 2, "accession": "P12345"},
            ],
            "cross_references": [{"xref_id": "X1", "xref_src": "SRC"}],
        }
        result = flatten_target_components(rec)

        assert "uniprot_accessions" in result
        assert result["uniprot_accessions"] != ""
        assert "P00533" in result["uniprot_accessions"]
        assert "P12345" in result["uniprot_accessions"]
        assert "component_count" in result
        assert result["component_count"] == 2
        assert "target_components__flat" in result
        assert result["target_components__flat"] != ""
        assert "cross_references__flat" in result
        assert result["cross_references__flat"] != ""

    def test_flatten_with_synonyms(self) -> None:
        """Test flattening with target_component_synonyms."""
        rec = {
            "target_chembl_id": "CHEMBL203",
            "target_components": [
                {
                    "component_id": 1,
                    "accession": "P00533",
                    "target_component_synonyms": [
                        {"syn_type": "GENE_SYMBOL", "component_synonym": "EGFR"},
                        {"syn_type": "UNIPROT", "component_synonym": "P00533"},
                    ],
                }
            ],
        }
        result = flatten_target_components(rec)

        assert "target_component_synonyms__flat" in result
        assert result["target_component_synonyms__flat"] != ""
        assert "syn_type" in result["target_component_synonyms__flat"]
        assert "component_synonym" in result["target_component_synonyms__flat"]
        assert "EGFR" in result["target_component_synonyms__flat"] or "P00533" in result["target_component_synonyms__flat"]

    def test_flatten_empty_components(self) -> None:
        """Test flattening with empty target_components."""
        rec: dict[str, Any] = {
            "target_chembl_id": "CHEMBL203",
            "target_components": [],
            "cross_references": [],
        }
        result = flatten_target_components(rec)

        assert result["uniprot_accessions"] == ""
        assert result["target_components__flat"] == ""
        assert result["target_component_synonyms__flat"] == ""
        assert result["cross_references__flat"] == ""
        assert result["component_count"] is None

    def test_flatten_none_components(self) -> None:
        """Test flattening with None target_components."""
        rec = {
            "target_chembl_id": "CHEMBL203",
            "target_components": None,
            "cross_references": None,
        }
        result = flatten_target_components(rec)

        assert result["uniprot_accessions"] == ""
        assert result["target_components__flat"] == ""
        assert result["target_component_synonyms__flat"] == ""
        assert result["cross_references__flat"] == ""

    def test_flatten_with_component_count_fallback(self) -> None:
        """Test flattening with component_count from top-level."""
        rec: dict[str, Any] = {
            "target_chembl_id": "CHEMBL203",
            "target_components": [],
            "component_count": 5,
        }
        result = flatten_target_components(rec)

        assert result["component_count"] == 5

    def test_flatten_duplicate_accessions(self) -> None:
        """Test flattening with duplicate accessions (should be deduplicated)."""
        rec = {
            "target_chembl_id": "CHEMBL203",
            "target_components": [
                {"component_id": 1, "accession": "P00533"},
                {"component_id": 2, "accession": "P00533"},  # duplicate
            ],
        }
        result = flatten_target_components(rec)

        assert result["component_count"] == 1
        # Check that accession appears only once in JSON
        accessions = json.loads(result["uniprot_accessions"])
        assert len(accessions) == 1
        assert accessions == ["P00533"]

    def test_flatten_sorted_accessions(self) -> None:
        """Test that accessions are sorted."""
        rec = {
            "target_chembl_id": "CHEMBL203",
            "target_components": [
                {"component_id": 1, "accession": "P99999"},
                {"component_id": 2, "accession": "P00001"},
            ],
        }
        result = flatten_target_components(rec)

        accessions = json.loads(result["uniprot_accessions"])
        assert accessions == ["P00001", "P99999"]  # sorted

    def test_serialize_target_arrays_with_uniprot_accessions(self) -> None:
        """Test that serialize_target_arrays extracts uniprot_accessions."""
        df = pd.DataFrame(
            {
                "target_chembl_id": ["CHEMBL203"],
                "target_components": [
                    [
                        {"component_id": 1, "accession": "P00533"},
                        {"component_id": 2, "accession": "P12345"},
                    ]
                ],
            }
        )
        mock_config = MagicMock()
        mock_config.transform.arrays_to_header_rows = ["target_components"]

        result = serialize_target_arrays(df, mock_config)

        assert "uniprot_accessions" in result.columns
        assert result["uniprot_accessions"].iloc[0] != ""
        accessions = json.loads(result["uniprot_accessions"].iloc[0])
        assert "P00533" in accessions
        assert "P12345" in accessions
        assert "component_count" in result.columns
        assert result["component_count"].iloc[0] == 2

