"""Golden regression tests for ChemblActivityPipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from tests.support.factories import load_sample_activity_dataframe
from tests.support.golden import (
    canonical_json,
    load_json_dict,
    load_yaml_dict,
    normalize_manifest_payload,
    normalize_meta_payload,
)

from bioetl.pipelines.chembl.activity.run import ChemblActivityPipeline

PIPELINE_CODE = "activity_chembl"
GOLDEN_VERSION = "v1"
DATASET_STEM = "activity_chembl_extended_20240101"


def _golden_root() -> Path:
    return Path(__file__).resolve().parent / PIPELINE_CODE / GOLDEN_VERSION


def _golden_paths() -> dict[str, Path]:
    root = _golden_root()
    return {
        "dataset": root / "dataset" / f"{DATASET_STEM}.csv",
        "meta": root / "meta" / f"{DATASET_STEM}_meta.yaml",
        "quality_report": root / "qc" / f"{DATASET_STEM}_quality_report.csv",
        "correlation_report": root / "qc" / f"{DATASET_STEM}_correlation_report.csv",
        "qc_metrics": root / "qc" / f"{DATASET_STEM}_qc.csv",
        "manifest": root / "manifest" / f"{DATASET_STEM}_run_manifest.json",
    }


@pytest.mark.golden
@pytest.mark.determinism
def test_activity_pipeline_golden_snapshot(
    pipeline_config_fixture,
) -> None:
    """ChemblActivityPipeline output must match committed golden artefacts."""

    pipeline_config_fixture.validation.schema_out = "bioetl.schemas.chembl_activity_schema:ActivitySchema"  # type: ignore[attr-defined]
    pipeline_config_fixture.determinism.sort.by = ["activity_id"]  # type: ignore[attr-defined]
    pipeline_config_fixture.determinism.sort.ascending = [True]  # type: ignore[attr-defined]
    pipeline_config_fixture.determinism.hashing.business_key_fields = ("activity_id",)  # type: ignore[attr-defined]

    golden_run_id = "golden-activity-v1"
    pipeline = ChemblActivityPipeline(config=pipeline_config_fixture, run_id=golden_run_id)  # type: ignore[arg-type]
    frame = load_sample_activity_dataframe()
    transformed = pipeline.transform(frame)
    validated = pipeline.validate(transformed)
    result = pipeline.write(validated, pipeline.pipeline_directory, extended=True)

    produced_paths: dict[str, Path | None] = {
        "dataset": result.write_result.dataset,
        "meta": result.write_result.metadata,
        "quality_report": result.write_result.quality_report,
        "correlation_report": result.write_result.correlation_report,
        "qc_metrics": result.write_result.qc_metrics,
        "manifest": result.manifest,
    }

    golden_paths = _golden_paths()
    for key in ("dataset", "quality_report", "correlation_report", "qc_metrics"):
        produced = produced_paths[key]
        golden = golden_paths[key]
        assert produced is not None, f"{key} path is missing"
        assert golden.exists(), f"golden {key} missing at {golden}"
        assert produced.read_bytes() == golden.read_bytes(), f"{key} artifact mismatch"

    produced_meta = normalize_meta_payload(
        load_yaml_dict(_require_path(produced_paths["meta"], "meta")),
    )
    golden_meta = normalize_meta_payload(load_yaml_dict(golden_paths["meta"]))
    assert canonical_json(produced_meta) == canonical_json(golden_meta), "meta.yaml mismatch"

    produced_manifest = normalize_manifest_payload(
        load_json_dict(_require_path(produced_paths["manifest"], "manifest")),
    )
    golden_manifest = normalize_manifest_payload(load_json_dict(golden_paths["manifest"]))
    produced_manifest = _filter_manifest_artifacts(produced_manifest, ignore=("meta",))
    golden_manifest = _filter_manifest_artifacts(golden_manifest, ignore=("meta",))
    assert (
        canonical_json(produced_manifest) == canonical_json(golden_manifest)
    ), "run manifest mismatch"


def _require_path(path: Path | None, label: str) -> Path:
    assert path is not None, f"{label} path is missing"
    return path


def _filter_manifest_artifacts(
    payload: dict[str, Any],
    *,
    ignore: tuple[str, ...],
) -> dict[str, Any]:
    """Return manifest payload without the specified artifact names."""

    filtered = dict(payload)
    artifacts = [
        item for item in filtered.get("artifacts", []) if item.get("name") not in ignore
    ]
    filtered["artifacts"] = sorted(artifacts, key=lambda item: (item.get("name", ""), item.get("path", "")))
    filtered["total_artifacts"] = len(filtered["artifacts"])
    return filtered

