from __future__ import annotations

import re
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


def test_no_exponential_format(monkeypatch, tmp_path: Path) -> None:
    frame = pd.DataFrame(
        {
            "activity_chembl_id": ["CHEMBL2", "CHEMBL1"],
            "value": [1.2345e-05, 12345678.9],
            "measurement": [0.0000001, 2500000.0],
        }
    )

    destination = tmp_path / "outputs" / "final" / "deterministic.csv"
    output_settings = OutputSettings(
        data_path=destination,
        qc_report_path=tmp_path / "qc.csv",
        correlation_path=tmp_path / "corr.csv",
        format="csv",
        csv=CsvFormatSettings(encoding="utf-8"),
        parquet=ParquetFormatSettings(compression=None),
    )
    determinism = DeterminismSettings(
        column_order=["index", "activity_chembl_id", "value", "measurement"],
        sort=SortSettings(by=["activity_chembl_id"], ascending=[True]),
    )

    monkeypatch.setenv("PIPELINE_VERSION", "2.0.0")
    monkeypatch.setenv("CHEMBL_RELEASE", "ChEMBL_32")
    monkeypatch.setenv("CHEMBL_RELEASE_SOURCE", "cli")

    write_deterministic_csv(frame, destination, determinism=determinism, output=output_settings)

    contents = destination.read_text(encoding="utf-8")
    assert not re.search(r"[eE][+-]?\d+", contents)
