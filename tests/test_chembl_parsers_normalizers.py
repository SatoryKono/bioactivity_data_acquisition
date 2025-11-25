from __future__ import annotations

import pandas as pd

from bioetl.sources.chembl.activity.normalizer import normalize_activity
from bioetl.sources.chembl.activity.parser import parse_activity_payload


def test_activity_parser_extracts_columns():
    payload = {
        "results": [
            {
                "activity_id": "ACT1",
                "assay_chembl_id": "ASSAY1",
                "target_chembl_id": "TGT1",
                "standard_value": "5.0",
                "standard_units": "nM",
            }
        ]
    }

    df = parse_activity_payload(payload)

    assert list(df.columns) == ["activity_id", "assay_id", "target_id", "value", "unit"]
    assert df.iloc[0].to_dict() == {
        "activity_id": "ACT1",
        "assay_id": "ASSAY1",
        "target_id": "TGT1",
        "value": "5.0",
        "unit": "nM",
    }


def test_activity_normalizer_coerces_types():
    raw_df = pd.DataFrame(
        {
            "activity_id": ["1"],
            "assay_id": ["2"],
            "target_id": [None],
            "value": ["1.5"],
            "unit": ["uM"],
        }
    )

    normalized = normalize_activity(raw_df)

    assert normalized["value"].dtype.kind == "f"
    assert normalized.loc[0, "value"] == 1.5
    assert normalized.loc[0, "business_key"] == "1"
    assert normalized.loc[0, "row_hash"]
