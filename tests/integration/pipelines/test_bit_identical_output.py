"""Integration tests for verifying bit-identical pipeline artefacts."""

from __future__ import annotations

import types
from pathlib import Path

import pandas as pd
import pytest

from bioetl.core.hashing import generate_hash_business_key, generate_hash_row
from bioetl.pipelines.base import PipelineBase

from tests.golden.helpers import snapshot_artifacts, verify_bit_identical_outputs

pytestmark = pytest.mark.integration


class _DeterministicPipeline(PipelineBase):
    """Minimal pipeline with deterministic extract/transform logic."""

    RAW_ROWS = (
        {
            "compound_id": "CHEMBL123",
            "assay_type": "B",
            "canonical_smiles": "C1=CC=CC=C1",
            "result_value": 1.5,
        },
        {
            "compound_id": "CHEMBL456",
            "assay_type": "F",
            "canonical_smiles": "C1=CC(=CC=C1)O",
            "result_value": 2.75,
        },
    )
    PIPELINE_VERSION = "0.0.0-test"
    SOURCE_SYSTEM = "determinism-harness"
    CHEMBL_RELEASE = "ChEMBL_TEST"
    EXTRACTED_AT = pd.Timestamp("2024-01-01T00:00:00Z")

    def extract(self, *args: object, **kwargs: object) -> pd.DataFrame:  # noqa: D401 - stub
        frame = pd.DataFrame(self.RAW_ROWS)
        frame["extracted_at"] = self.EXTRACTED_AT.isoformat()
        return frame

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:  # noqa: D401 - stub
        working = df.copy()
        working["hash_business_key"] = working["compound_id"].apply(generate_hash_business_key)

        hash_inputs = working[
            [
                "compound_id",
                "assay_type",
                "canonical_smiles",
                "result_value",
                "extracted_at",
            ]
        ].to_dict("records")
        working["hash_row"] = [generate_hash_row(payload) for payload in hash_inputs]

        working.insert(0, "index", range(1, len(working) + 1))
        working["pipeline_version"] = self.PIPELINE_VERSION
        working["source_system"] = self.SOURCE_SYSTEM
        working["chembl_release"] = self.CHEMBL_RELEASE

        ordered_columns = [
            "index",
            "hash_row",
            "hash_business_key",
            "pipeline_version",
            "source_system",
            "chembl_release",
            "compound_id",
            "assay_type",
            "canonical_smiles",
            "result_value",
            "extracted_at",
        ]
        extra_columns = [column for column in working.columns if column not in ordered_columns]
        ordered_frame = working[ordered_columns + extra_columns]

        self.set_export_metadata_from_dataframe(
            ordered_frame,
            pipeline_version=self.PIPELINE_VERSION,
            source_system=self.SOURCE_SYSTEM,
            chembl_release=self.CHEMBL_RELEASE,
        )
        self.set_qc_metrics({"row_count": len(ordered_frame)})
        self.refresh_validation_issue_summary()
        return ordered_frame

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:  # noqa: D401 - stub
        return df

    def close_resources(self) -> None:  # noqa: D401 - stub
        """No-op resource cleanup for deterministic stub."""
        return None


def _make_config() -> types.SimpleNamespace:
    pipeline_section = types.SimpleNamespace(
        name="deterministic", entity="deterministic", version=_DeterministicPipeline.PIPELINE_VERSION
    )
    qc_section = types.SimpleNamespace(severity_threshold="error")
    determinism_section = types.SimpleNamespace(
        column_order=[],
        float_precision=6,
        datetime_format="iso8601",
        sort=types.SimpleNamespace(by=[], ascending=[]),
        hash_policy_version=None,
    )
    sources = {"chembl": types.SimpleNamespace(enabled=True)}
    return types.SimpleNamespace(
        pipeline=pipeline_section,
        qc=qc_section,
        cli={},
        determinism=determinism_section,
        config_hash="deterministic-config-hash",
        sources=sources,
    )


@pytest.mark.determinism
def test_pipeline_outputs_are_bit_identical(tmp_path: Path, frozen_time) -> None:
    """Running the same pipeline twice should yield bit-identical artefacts."""

    config = _make_config()
    output_path = tmp_path / "materialised" / "datasets" / "deterministic.csv"

    first_pipeline = _DeterministicPipeline(config, run_id="bit-identical")
    first_artifacts = first_pipeline.run(output_path, extended=True)
    first_snapshot = snapshot_artifacts(first_artifacts, tmp_path / "snapshot_first")

    second_pipeline = _DeterministicPipeline(config, run_id="bit-identical")
    second_artifacts = second_pipeline.run(output_path, extended=True)
    second_snapshot = snapshot_artifacts(second_artifacts, tmp_path / "snapshot_second")

    identical, errors = verify_bit_identical_outputs(first_snapshot, second_snapshot)
    assert identical, "Outputs diverged:\n- " + "\n- ".join(errors)
