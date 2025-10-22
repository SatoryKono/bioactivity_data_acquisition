from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from library.config import (
    CsvFormatSettings,
    DeterminismSettings,
    OutputSettings,
    ParquetFormatSettings,
    SortSettings,
)
from library.etl.load import write_deterministic_csv


def test_export_generates_determinism_report(monkeypatch, tmp_path: Path) -> None:
    frame = pd.DataFrame(
        {
            "activity_chembl_id": ["CHEMBL1", "CHEMBL2"],
            "value": [10.0, 20.0],
        }
    )

    destination = tmp_path / "outputs" / "final" / "report.csv"
    output_settings = OutputSettings(
        data_path=destination,
        qc_report_path=tmp_path / "qc.csv",
        correlation_path=tmp_path / "corr.csv",
        format="csv",
        csv=CsvFormatSettings(encoding="utf-8"),
        parquet=ParquetFormatSettings(compression=None),
    )
    determinism = DeterminismSettings(
        column_order=["index", "activity_chembl_id", "value"],
        sort=SortSettings(by=["activity_chembl_id"], ascending=[True]),
    )

    monkeypatch.setenv("PIPELINE_VERSION", "3.0.0")
    monkeypatch.setenv("CHEMBL_RELEASE", "ChEMBL_32")
    monkeypatch.setenv("CHEMBL_RELEASE_SOURCE", "cli")

    write_deterministic_csv(frame, destination, determinism=determinism, output=output_settings)

    report_path = tmp_path / "reports" / "export_determinism.json"
    assert report_path.exists()
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report == {"status": "PASSED"}

    meta_path = tmp_path / "outputs" / "meta" / "meta.yaml"
    assert meta_path.exists()
