from __future__ import annotations

from pathlib import Path

import pandas as pd

from library.activity.config import load_activity_config
from library.activity.pipeline import ActivityETLResult, write_activity_outputs


def test_write_activity_outputs_tmp(tmp_path: Path):
    cfg = load_activity_config(None, overrides={})

    df = pd.DataFrame([
        {"source": "chembl", "retrieved_at": "2024-01-01T00:00:00Z", "activity_value": 1000.0, "activity_unit": "nM"}
    ])
    qc = pd.DataFrame([{"metric": "row_count", "value": 1}])
    result = ActivityETLResult(activity=df, qc=qc, meta={"pipeline": "activity"})

    out = write_activity_outputs(result, tmp_path, date_tag="20240101", config=cfg)

    assert out["activity"].exists()
    assert out["qc"].exists()
    assert out["meta"].exists()

