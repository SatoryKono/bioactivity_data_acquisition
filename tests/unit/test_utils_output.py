"""Unit tests for :mod:`bioetl.utils.output`."""

import pandas as pd

from bioetl.utils.output import finalize_output_dataset


def test_finalize_output_dataset_populates_metadata_and_hashes():
    """Metadata values should be applied without overwriting non-null entries."""

    df = pd.DataFrame(
        [
            {
                "id": "CHEMBL002",
                "source_system": None,
                "chembl_release": None,
                "payload": "x",
            },
            {
                "id": "CHEMBL001",
                "source_system": "DOCUMENT_FALLBACK",
                "chembl_release": "ChEMBL_OLD",
                "payload": "y",
            },
        ]
    )

    result = finalize_output_dataset(
        df,
        business_key="id",
        sort_by=["id"],
        metadata={
            "pipeline_version": "9.9.9",
            "source_system": "chembl",
            "chembl_release": "ChEMBL_NEW",
            "extracted_at": "2024-01-01T00:00:00+00:00",
            "custom_flag": True,
        },
    )

    assert result["id"].tolist() == ["CHEMBL001", "CHEMBL002"]
    assert result.loc[0, "source_system"] == "DOCUMENT_FALLBACK"
    assert result.loc[1, "source_system"] == "chembl"
    assert result.loc[0, "chembl_release"] == "ChEMBL_OLD"
    assert result.loc[1, "chembl_release"] == "ChEMBL_NEW"
    assert result["pipeline_version"].unique().tolist() == ["9.9.9"]
    assert result["extracted_at"].unique().tolist() == ["2024-01-01T00:00:00+00:00"]
    assert result.loc[0, "custom_flag"] is True
    assert "hash_business_key" in result.columns
    assert "hash_row" in result.columns
    assert "index" in result.columns


def test_finalize_output_dataset_can_override_metadata():
    """The overwrite flag should force metadata replacement when requested."""

    df = pd.DataFrame(
        [
            {"id": "CHEMBL1", "source_system": "fallback"},
            {"id": "CHEMBL2", "source_system": "legacy"},
        ]
    )

    result = finalize_output_dataset(
        df,
        business_key="id",
        metadata={"source_system": "chembl"},
        overwrite_metadata=("pipeline_version", "extracted_at", "source_system"),
    )

    assert result["source_system"].tolist() == ["chembl", "chembl"]
