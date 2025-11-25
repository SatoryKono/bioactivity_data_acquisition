from __future__ import annotations

from pathlib import Path

import pandas as pd
import yaml

from bioetl.core.io.output import OutputWriter


def _build_df() -> pd.DataFrame:
    return pd.DataFrame({"chembl_id": ["1", "2"], "value": [1.0, 2.0]})


def test_output_writer_respects_configured_paths(tmp_path: Path) -> None:
    config = {
        "output": {
            "dataset_path": "custom/data.csv",
            "quality_report_path": "custom/quality.csv",
            "meta_path": "meta/info.yaml",
            "manifest_path": "manifest/manifest.yaml",
            "logs_dir": "logs",
            "log_file": "chembl.log",
        }
    }
    writer = OutputWriter(tmp_path, config)

    artifacts = writer.write_outputs(
        df=_build_df(),
        stem="assay_chembl",
        run_id="run-id",
        pipeline_name="AssayPipeline",
        entity_name="assay",
        quality_report=pd.DataFrame([{"rows": 2, "columns": 2, "missing_values": 0}]),
    )

    assert artifacts.data_path == (tmp_path / "custom/data.csv").resolve()
    assert artifacts.quality_report_path == (tmp_path / "custom/quality.csv").resolve()
    assert artifacts.meta_path == (tmp_path / "meta/info.yaml").resolve()
    assert artifacts.manifest_path == (tmp_path / "manifest/manifest.yaml").resolve()
    assert artifacts.quality_report_path.exists()
    assert (tmp_path / "logs" / "chembl.log").exists()

    manifest = yaml.safe_load(artifacts.manifest_path.read_text())
    assert manifest["artifacts"]["dataset"] == artifacts.data_path.name


def test_output_writer_default_paths(tmp_path: Path) -> None:
    writer = OutputWriter(tmp_path, {"output": {"logs_dir": "logs"}})

    artifacts = writer.write_outputs(
        df=_build_df(),
        stem="target_chembl",
        run_id="run-id",
        pipeline_name="TargetPipeline",
        entity_name="target",
        quality_report=pd.DataFrame([{"rows": 2, "columns": 2, "missing_values": 0}]),
    )

    assert artifacts.data_path.name.startswith("target_chembl_all_")
    assert artifacts.quality_report_path.name == "target_chembl_quality_report.csv"
    meta = yaml.safe_load(artifacts.meta_path.read_text())
    assert meta["rows"] == 2
    assert meta["entity"] == "target"
