from __future__ import annotations

import pandas as pd
import pytest

from bioactivity.etl.transform import normalize_bioactivity_data


def test_normalize_units_converts_all_to_nm(sample_frame: pd.DataFrame) -> None:
    sample_frame = sample_frame.rename(
        columns={
            "assay_id": "compound_id",
            "standard_value": "activity_value",
            "standard_units": "activity_units",
            "activity_comment": "target_pref_name",
        }
    )
    sample_frame = sample_frame.drop(columns=["molecule_chembl_id"], errors="ignore")
    sample_frame["source"] = "chembl"
    sample_frame["retrieved_at"] = "2024-01-01T00:00:00Z"
    sample_frame["smiles"] = "C"
    sample_frame["target_pref_name"] = sample_frame["target_pref_name"].fillna("Unknown")
    result = normalize_bioactivity_data(sample_frame)
    assert set(result["activity_unit"].unique()) == {"nM"}
    expected = [1.5, 2_500.0]
    assert result["activity_value"].tolist() == pytest.approx(expected, rel=1e-6)


def test_normalize_units_rejects_unknown_unit(sample_frame: pd.DataFrame) -> None:
    sample_frame = sample_frame.rename(
        columns={
            "assay_id": "compound_id",
            "standard_value": "activity_value",
            "standard_units": "activity_units",
            "activity_comment": "target_pref_name",
        }
    )
    sample_frame = sample_frame.drop(columns=["molecule_chembl_id"], errors="ignore")
    sample_frame.loc[0, "activity_units"] = "mg/mL"
    sample_frame["source"] = "chembl"
    sample_frame["retrieved_at"] = "2024-01-01T00:00:00Z"
    sample_frame["smiles"] = "C"
    sample_frame["target_pref_name"] = sample_frame["target_pref_name"].fillna("Unknown")
    with pytest.raises(ValueError):
        normalize_bioactivity_data(sample_frame)
