"""Golden smoke tests for CLI pipeline execution."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path

import click
import pandas as pd
import pytest
import typer
from click.testing import CliRunner
from pandera.typing import Series
from typer.main import get_command

from bioetl.cli.command import PipelineCommandConfig, create_pipeline_command
from bioetl.config.loader import load_config
from bioetl.core.hashing import generate_hash_business_key, generate_hash_row
from bioetl.pipelines.base import PipelineBase
from bioetl.schemas.base import BaseSchema
from bioetl.schemas.registry import schema_registry
from scripts import PIPELINE_COMMAND_REGISTRY

RUNNER = CliRunner()


class GoldenStubPipeline(PipelineBase):
    """Minimal deterministic pipeline used for CLI golden tests."""

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
    TIMESTAMP = pd.Timestamp("2024-01-01T00:00:00Z")
    PIPELINE_VERSION = "0.0.0-test"
    SOURCE_SYSTEM = "unit-test-harness"
    CHEMBL_RELEASE = "ChEMBL_TEST_RELEASE"

    def extract(self, *args: object, **kwargs: object) -> pd.DataFrame:  # noqa: D401 - test stub
        frame = pd.DataFrame(self.RAW_ROWS)
        frame["extracted_at"] = self.TIMESTAMP
        return frame

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:  # noqa: D401 - test stub
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

        ordered = [
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
        extra_columns = [column for column in working.columns if column not in ordered]
        working = working[ordered + extra_columns]

        self.set_export_metadata_from_dataframe(
            working,
            pipeline_version=self.PIPELINE_VERSION,
            source_system=self.SOURCE_SYSTEM,
            chembl_release=self.CHEMBL_RELEASE,
        )
        self.set_qc_metrics({"row_count": len(working)})
        self.refresh_validation_issue_summary()
        return working

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:  # noqa: D401 - test stub
        return df


class ColumnValidationStubPipeline(GoldenStubPipeline):
    """Stub pipeline ensuring CLI column validation succeeds."""

    OUTPUT_COLUMNS = [
        "index",
        "hash_row",
        "hash_business_key",
        "pipeline_version",
        "run_id",
        "source_system",
        "chembl_release",
        "compound_id",
        "assay_type",
        "canonical_smiles",
        "result_value",
        "extracted_at",
    ]

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:  # noqa: D401 - test stub
        base_frame = super().transform(df)
        working = base_frame.copy()
        working["run_id"] = self.run_id

        ordered = list(self.OUTPUT_COLUMNS)
        extra_columns = [column for column in working.columns if column not in ordered]
        ordered_frame = working[ordered + extra_columns]

        self.set_export_metadata_from_dataframe(
            ordered_frame,
            pipeline_version=self.PIPELINE_VERSION,
            source_system=self.SOURCE_SYSTEM,
            chembl_release=self.CHEMBL_RELEASE,
            column_order=list(ordered_frame.columns),
        )

        return ordered_frame


def _build_cli_command(pipeline_cls: type[PipelineBase]) -> tuple[click.Command, PipelineCommandConfig]:
    """Create a Click command hosting the pipeline using ``pipeline_cls``."""

    registry_entry = PIPELINE_COMMAND_REGISTRY["assay"]
    config = replace(
        registry_entry,
        pipeline_factory=lambda: pipeline_cls,
        default_input=None,
    )

    app = typer.Typer()
    app.command(name=config.pipeline_name)(create_pipeline_command(config))
    command = get_command(app)
    return command, config


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.mark.integration
@pytest.mark.golden
def test_cli_run_matches_expected_csv_hash(tmp_path: Path) -> None:
    """Full CLI execution should materialise a dataset matching the golden hash."""

    command, config = _build_cli_command(GoldenStubPipeline)
    output_dir = tmp_path / "actual"

    args = [
        "--config",
        str(config.default_config.resolve()),
        "--output-dir",
        str(output_dir),
        "--no-validate-columns",
    ]
    result = RUNNER.invoke(command, args, catch_exceptions=False)

    assert result.exit_code == 0, result.stdout
    datasets = sorted(output_dir.glob("*.csv"))
    assert datasets, "CLI should create a dataset CSV"
    actual_path = datasets[0]
    actual_hash = _sha256(actual_path)

    expected_dir = tmp_path / "expected"
    expected_dir.mkdir()
    config_obj = load_config(config.default_config.resolve())
    pipeline = GoldenStubPipeline(config_obj, "expected-run")
    artifacts = pipeline.run(expected_dir / "expected.csv")
    expected_hash = _sha256(artifacts.dataset)

    assert actual_hash == expected_hash


@pytest.mark.integration
def test_cli_dry_run_reports_config_hash(tmp_path: Path) -> None:
    """``--dry-run`` mode should surface the resolved configuration hash."""

    command, config = _build_cli_command(GoldenStubPipeline)
    config_path = config.default_config.resolve()
    overrides = {
        "cli": {
            "fail_on_schema_drift": True,
            "extended": False,
            "mode": config.default_mode,
            "dry_run": True,
            "verbose": False,
        }
    }
    config_hash = load_config(config_path, overrides=overrides).config_hash

    args = [
        "--config",
        str(config_path),
        "--dry-run",
        "--no-validate-columns",
    ]
    result = RUNNER.invoke(command, args, catch_exceptions=False)

    assert result.exit_code == 0, result.stdout
    assert "[DRY-RUN] Configuration loaded successfully." in result.stdout
    assert f"Config hash: {config_hash}" in result.stdout


@pytest.mark.integration
@pytest.mark.golden
def test_cli_validates_columns_and_generates_report(tmp_path: Path) -> None:
    """Full CLI execution should validate columns and emit a report."""

    command, config = _build_cli_command(ColumnValidationStubPipeline)
    config_path = config.default_config.resolve()
    output_dir = tmp_path / "validated"

    class ColumnValidationSchema(BaseSchema):
        """Minimal schema mirroring the stub pipeline output."""

        compound_id: Series[str]
        assay_type: Series[str]
        canonical_smiles: Series[str]
        result_value: Series[float]

        _column_order = ColumnValidationStubPipeline.OUTPUT_COLUMNS

    registry_snapshot = schema_registry._registry.get("assay", {}).copy()

    try:
        schema_registry.register("assay", "99.0.0", ColumnValidationSchema)

        args = [
            "--config",
            str(config_path),
            "--output-dir",
            str(output_dir),
        ]
        result = RUNNER.invoke(command, args, catch_exceptions=False)

        assert result.exit_code == 0, result.stdout
        assert "Колонки соответствуют требованиям" in result.stdout
        assert "Критические несоответствия" not in result.stdout

        datasets = sorted(output_dir.glob("*.csv"))
        assert datasets, "CLI should materialise a dataset CSV"
        dataset_path = datasets[0]
        frame = pd.read_csv(dataset_path)
        assert frame.shape[0] == len(ColumnValidationStubPipeline.RAW_ROWS)
        assert list(frame.columns[: len(ColumnValidationStubPipeline.OUTPUT_COLUMNS)]) == ColumnValidationStubPipeline.OUTPUT_COLUMNS

        report_dir = output_dir / "validation_reports"
        json_report = report_dir / "column_comparison_report.json"
        md_report = report_dir / "column_comparison_report.md"
        assert json_report.exists(), "JSON validation report should be generated"
        assert md_report.exists(), "Markdown validation report should be generated"

        report_payload = json.loads(json_report.read_text(encoding="utf-8"))
        summary = report_payload.get("summary", {})
        assert summary.get("matching_entities") == 1
        assert summary.get("entities_with_issues") == 0
    finally:
        if registry_snapshot:
            schema_registry._registry["assay"] = registry_snapshot
        else:
            schema_registry._registry.pop("assay", None)

