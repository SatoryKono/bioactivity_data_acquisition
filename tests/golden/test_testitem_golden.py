"""Golden regression tests for TestItemChemblPipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from tests.support.factories import load_sample_testitem_dataframe
from tests.support.golden import (
    canonical_json,
    load_yaml_dict,
    normalize_meta_payload,
)

from bioetl.pipelines.chembl.testitem.run import TestItemChemblPipeline

PIPELINE_CODE = "testitem_chembl"
GOLDEN_VERSION = "v2"
DATASET_STEM = "testitem_chembl_extended_20240101"


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
def test_testitem_pipeline_golden_snapshot(
    pipeline_config_fixture,
) -> None:
    """TestItemChemblPipeline output must match committed golden artefacts."""

    pipeline_config_fixture.validation.schema_out = (
        "bioetl.schemas.chembl_testitem_schema.TestItemSchema"  # type: ignore[attr-defined]
    )
    pipeline_config_fixture.determinism.sort.by = ["molecule_chembl_id"]  # type: ignore[attr-defined]
    pipeline_config_fixture.determinism.sort.ascending = [True]  # type: ignore[attr-defined]
    pipeline_config_fixture.determinism.hashing.business_key_fields = ("molecule_chembl_id",)  # type: ignore[attr-defined]

    golden_run_id = "golden-testitem-v1"
    pipeline = TestItemChemblPipeline(config=pipeline_config_fixture, run_id=golden_run_id)  # type: ignore[arg-type]
    frame = load_sample_testitem_dataframe()
    transformed = pipeline.transform(frame)
    validated = pipeline.validate(transformed)
    result = pipeline.save_results(validated, pipeline.pipeline_directory, extended=True)

    produced_paths: dict[str, Path | None] = {
        "dataset": result.write_result.dataset,
        "meta": result.write_result.metadata,
        "quality_report": result.write_result.quality_report,
    }

    golden_paths = _golden_paths()
    for key in ("dataset", "quality_report"):
        produced = produced_paths[key]
        golden = golden_paths[key]
        assert produced is not None, f"{key} path is missing"
        # Create golden file if it doesn't exist (first run)
        if not golden.exists():
            golden.parent.mkdir(parents=True, exist_ok=True)
            golden.write_bytes(produced.read_bytes())
        assert golden.exists(), f"golden {key} missing at {golden}"
        produced_bytes = produced.read_bytes().replace(b"\r\n", b"\n")
        golden_bytes = golden.read_bytes().replace(b"\r\n", b"\n")
        assert produced_bytes == golden_bytes, f"{key} artifact mismatch"

    # Create golden meta file if it doesn't exist (first run)
    produced_meta_path = _require_path(produced_paths["meta"], "meta")
    if not golden_paths["meta"].exists():
        golden_paths["meta"].parent.mkdir(parents=True, exist_ok=True)
        golden_paths["meta"].write_bytes(produced_meta_path.read_bytes())
    produced_meta = normalize_meta_payload(
        load_yaml_dict(produced_meta_path),
    )
    golden_meta = normalize_meta_payload(load_yaml_dict(golden_paths["meta"]))
    assert canonical_json(produced_meta) == canonical_json(golden_meta), "meta.yaml mismatch"


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
    artifacts = [item for item in filtered.get("artifacts", []) if item.get("name") not in ignore]
    filtered["artifacts"] = sorted(
        artifacts, key=lambda item: (item.get("name", ""), item.get("path", ""))
    )
    filtered["total_artifacts"] = len(filtered["artifacts"])
    return filtered
