"""Data-centric pytest fixtures used across tests."""

from __future__ import annotations

import json
from typing import Any

import pandas as pd
import pytest


__all__ = [
    "sample_activity_data",
    "sample_activity_data_raw",
    "sample_chembl_api_response",
    "sample_chembl_status_response",
]


@pytest.fixture  # type: ignore[misc]
def sample_activity_data() -> pd.DataFrame:
    """Sample activity DataFrame for testing."""
    activity_properties_valid = json.dumps(
        [
            {
                "type": "IC50",
                "relation": "=",
                "units": "nM",
                "value": 10.5,
                "text_value": None,
                "result_flag": True,
            }
        ]
    )

    df = pd.DataFrame(
        {
            "activity_id": [1, 2, 3],
            "assay_chembl_id": ["CHEMBL100", "CHEMBL101", "CHEMBL102"],
            "assay_type": ["B", "F", None],
            "assay_description": ["Binding assay", "Functional assay", None],
            "assay_organism": ["Homo sapiens", "Mus musculus", None],
            "assay_tax_id": pd.Series([9606, 10090, None], dtype="Int64"),
            "testitem_chembl_id": ["CHEMBL1", "CHEMBL2", "CHEMBL3"],
            "molecule_chembl_id": ["CHEMBL1", "CHEMBL2", "CHEMBL3"],
            "parent_molecule_chembl_id": [None, "CHEMBL1", None],
            "molecule_pref_name": ["Molecule 1", "Molecule 2", None],
            "target_chembl_id": ["CHEMBL200", "CHEMBL201", "CHEMBL202"],
            "target_pref_name": ["Target 1", "Target 2", None],
            "document_chembl_id": ["CHEMBL300", "CHEMBL301", "CHEMBL302"],
            "record_id": pd.Series([100, 101, None], dtype="Int64"),
            "src_id": pd.Series([1, 2, None], dtype="Int64"),
            "type": ["IC50", "EC50", "Ki"],
            "relation": ["=", ">", "<="],
            "value": [10.5, 20.0, 5.3],
            "units": ["nM", "μM", "mM"],
            "standard_type": ["IC50", "EC50", "Ki"],
            "standard_relation": ["=", ">", "<="],
            "standard_value": [10.5, 20.0, 5.3],
            "standard_upper_value": [None, 25.0, None],
            "standard_units": ["nM", "μM", "mM"],
            "standard_text_value": [None, None, "5.3"],
            "standard_flag": [0, 1, 0],
            "upper_value": [None, 25.0, None],
            "lower_value": [None, 15.0, None],
            "pchembl_value": [7.98, 6.70, 8.28],
            "published_type": ["IC50", None, "Ki"],
            "published_relation": ["=", None, "<="],
            "published_value": [10.5, None, 5.3],
            "published_units": ["nM", None, "mM"],
            "uo_units": [None, "UO_0000001", None],
            "qudt_units": [None, None, "QUDT_0000001"],
            "text_value": [None, "Text value", None],
            "activity_comment": [None, "Test comment", None],
            "bao_endpoint": ["BAO_0000001", "BAO_0000002", None],
            "bao_format": ["BAO_0000003", None, "BAO_0000004"],
            "bao_label": ["Binding", "Functional", None],
            "canonical_smiles": ["CCO", "CCN", "CCC"],
            "ligand_efficiency": [None, '{"LE": 0.5}', None],
            "target_organism": ["Homo sapiens", "Mus musculus", None],
            "target_tax_id": pd.Series([9606, 10090, 9606], dtype="Int64"),
            "data_validity_comment": [None, None, "Validated"],
            "data_validity_description": [None, "Validated description", None],
            "potential_duplicate": [False, True, None],
            "activity_properties": [None, activity_properties_valid, None],
            "compound_key": ["key1", "key2", "key3"],
        }
    )
    df["target_tax_id"] = pd.Series([9606, 10090, 9606], dtype="Int64")
    df["assay_tax_id"] = pd.Series([9606, 10090, None], dtype="Int64")
    df["record_id"] = pd.Series([100, 101, None], dtype="Int64")
    df["src_id"] = pd.Series([1, 2, None], dtype="Int64")
    return df


@pytest.fixture  # type: ignore[misc]
def sample_activity_data_raw() -> list[dict[str, Any]]:
    """Raw activity data as it would come from ChEMBL API."""
    return [
        {
            "activity_id": 1,
            "molecule_chembl_id": "CHEMBL1",
            "assay_chembl_id": "CHEMBL100",
            "assay_type": "B",
            "assay_description": "Binding assay",
            "assay_organism": "Homo sapiens",
            "assay_tax_id": 9606,
            "testitem_chembl_id": "CHEMBL1",
            "target_chembl_id": "CHEMBL200",
            "target_pref_name": "Target 1",
            "document_chembl_id": "CHEMBL300",
            "record_id": 100,
            "src_id": 1,
            "standard_type": "IC50",
            "standard_relation": "=",
            "standard_value": 10.5,
            "standard_upper_value": None,
            "standard_units": "nM",
            "pchembl_value": 7.98,
            "published_type": "IC50",
            "published_relation": "=",
            "published_value": 10.5,
            "published_units": "nM",
            "bao_endpoint": "BAO_0000001",
            "bao_format": "BAO_0000003",
            "bao_label": "Binding",
            "canonical_smiles": "CCO",
            "target_organism": "Homo sapiens",
            "target_tax_id": 9606,
            "data_validity_description": None,
            "potential_duplicate": 0,
        },
        {
            "activity_id": 2,
            "molecule_chembl_id": "CHEMBL2",
            "parent_molecule_chembl_id": "CHEMBL1",
            "molecule_pref_name": "Molecule 2",
            "assay_chembl_id": "CHEMBL101",
            "assay_type": "F",
            "assay_description": "Functional assay",
            "assay_organism": "Mus musculus",
            "assay_tax_id": 10090,
            "testitem_chembl_id": "CHEMBL2",
            "target_chembl_id": "CHEMBL201",
            "target_pref_name": "Target 2",
            "document_chembl_id": "CHEMBL301",
            "record_id": 101,
            "src_id": 2,
            "standard_type": "EC50",
            "standard_relation": ">",
            "standard_value": 20.0,
            "standard_upper_value": 25.0,
            "standard_units": "μM",
            "pchembl_value": 6.70,
            "published_type": None,
            "published_relation": None,
            "published_value": None,
            "published_units": None,
            "uo_units": "UO_0000001",
            "text_value": "Text value",
            "bao_endpoint": "BAO_0000002",
            "bao_label": "Functional",
            "ligand_efficiency": {"LE": 0.5},
            "target_organism": "Mus musculus",
            "target_tax_id": 10090,
            "data_validity_description": "Validated description",
            "activity_properties": {"property": "value"},
            "potential_duplicate": 1,
        },
    ]


@pytest.fixture  # type: ignore[misc]
def sample_chembl_api_response() -> dict[str, Any]:
    """Sample ChEMBL API response structure."""
    return {
        "page_meta": {
            "offset": 0,
            "limit": 25,
            "count": 2,
            "next": None,
        },
        "activities": [
            {
                "activity_id": 1,
                "molecule_chembl_id": "CHEMBL1",
                "assay_chembl_id": "CHEMBL100",
                "target_chembl_id": "CHEMBL200",
                "document_chembl_id": "CHEMBL300",
                "standard_type": "IC50",
                "standard_relation": "=",
                "standard_value": 10.5,
                "standard_units": "nM",
            },
            {
                "activity_id": 2,
                "molecule_chembl_id": "CHEMBL2",
                "assay_chembl_id": "CHEMBL101",
                "target_chembl_id": "CHEMBL201",
                "document_chembl_id": "CHEMBL301",
                "standard_type": "EC50",
                "standard_relation": ">",
                "standard_value": 20.0,
                "standard_units": "μM",
            },
        ],
    }


@pytest.fixture  # type: ignore[misc]
def sample_chembl_status_response() -> dict[str, Any]:
    """Sample ChEMBL status API response."""
    return {
        "chembl_release": "33",
        "chembl_db_version": "33",
        "release_date": "2024-01-01",
    }
