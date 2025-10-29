"""Integration tests ensuring unified QC exports across pipelines."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from bioetl.config.loader import load_config
from bioetl.pipelines.activity import ActivityPipeline
from bioetl.pipelines.document import DocumentPipeline
from bioetl.pipelines.target import TargetPipeline


PIPELINE_MATRIX = (
    (ActivityPipeline, Path("configs/pipelines/activity.yaml")),
    (DocumentPipeline, Path("configs/pipelines/document.yaml")),
    (TargetPipeline, Path("configs/pipelines/target.yaml")),
)


@pytest.mark.integration
def test_qc_report_format_and_additional_tables_consistency(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """QC report columns and additional tables should align across pipelines."""

    report_columns: list[tuple[str, ...]] = []
    summary_keys: list[tuple[str, ...]] = []

    for pipeline_cls, config_path in PIPELINE_MATRIX:
        if hasattr(pipeline_cls, "_get_chembl_release"):
            monkeypatch.setattr(pipeline_cls, "_get_chembl_release", lambda self: "TEST_RELEASE", raising=False)

        config = load_config(config_path)
        pipeline = pipeline_cls(config, "qc-test-run")

        frame = pd.DataFrame({"value": [1, None], "flag": [True, False]})
        entity_name = config.pipeline.entity

        pipeline.set_qc_metrics({"completeness": 1.0})
        pipeline.update_qc_summary_section("row_counts", {entity_name: int(len(frame))})
        pipeline.update_qc_summary_section(
            "datasets",
            {entity_name: {"rows": int(len(frame))}},
        )
        pipeline.record_validation_issue({"metric": "dummy", "severity": "warning"})
        pipeline.update_qc_validation_summary()

        pipeline.build_export_metadata(
            frame,
            pipeline_version="1.0.0",
            source_system=entity_name,
            chembl_release="TEST_RELEASE",
            column_order=list(frame.columns),
        )

        pipeline.register_additional_tables(
            [
                (
                    "qc_debug_table",
                    pd.DataFrame({"note": ["ok"]}),
                    Path("qc") / f"{entity_name}_debug.csv",
                )
            ]
        )

        output_path = tmp_path / f"{entity_name}_dataset.csv"
        artifacts = pipeline.export(frame, output_path, extended=True)

        report = pd.read_csv(artifacts.quality_report)
        report_columns.append(tuple(report.columns))

        assert "qc_debug_table" in artifacts.additional_datasets
        assert artifacts.additional_datasets["qc_debug_table"].exists()

        assert artifacts.qc_summary is not None and artifacts.qc_summary.exists()
        summary_payload = json.loads(artifacts.qc_summary.read_text(encoding="utf-8"))
        summary_keys.append(tuple(sorted(summary_payload.keys())))
        assert "row_counts" in summary_payload
        assert "validation_issue_counts" in summary_payload

    assert len(set(report_columns)) == 1, "QC report columns diverged between pipelines"
    assert len(set(summary_keys)) == 1, "QC summary structure diverged between pipelines"
