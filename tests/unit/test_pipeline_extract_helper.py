"""Tests for the shared PipelineBase.read_input_table helper."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from bioetl.config.loader import load_config
from bioetl.pipelines import AssayPipeline, TargetPipeline
from bioetl.pipelines.base import PipelineBase
from bioetl.config.models import PipelineConfig


@pytest.fixture()
def _assay_config(tmp_path: Path) -> PipelineConfig:
    """Return a temporary input directory pre-populated with assay.csv."""

    config = load_config("configs/pipelines/assay.yaml")
    updated = config.model_copy(
        update={
            "paths": config.paths.model_copy(update={"input_root": tmp_path}),
        }
    )

    dataframe = pd.DataFrame(
        {
            "assay_chembl_id": ["CHEMBL1", "CHEMBL2", "CHEMBL3"],
        }
    )
    dataframe.to_csv(tmp_path / "assay.csv", index=False)

    return updated


@pytest.fixture()
def _target_config(tmp_path: Path) -> PipelineConfig:
    """Return a target pipeline config using the temporary input directory."""

    config = load_config("configs/pipelines/target.yaml")
    updated = config.model_copy(
        update={
            "paths": config.paths.model_copy(update={"input_root": tmp_path}),
        }
    )
    return updated


def test_assay_extract_uses_helper_and_applies_limit(
    monkeypatch: pytest.MonkeyPatch,
    _assay_config: PipelineConfig,
) -> None:
    """AssayPipeline.extract should delegate reading to the shared helper."""

    monkeypatch.setattr(AssayPipeline, "_initialize_status", lambda self: None)

    captured: dict[str, object] = {}
    original = PipelineBase.read_input_table

    def _spy(self: PipelineBase, **kwargs):
        captured.update(kwargs)
        return original(self, **kwargs)

    monkeypatch.setattr(PipelineBase, "read_input_table", _spy)

    pipeline = AssayPipeline(_assay_config, run_id="unit-test")
    pipeline.runtime_options["limit"] = 2

    result = pipeline.extract()

    assert len(result) == 2
    assert captured["default_filename"] == Path("assay.csv")
    assert captured["expected_columns"] == ["assay_chembl_id"]
    assert captured.get("dtype") == "string"


def test_target_extract_handles_missing_file_via_helper(
    monkeypatch: pytest.MonkeyPatch,
    _target_config: PipelineConfig,
) -> None:
    """TargetPipeline should rely on the helper for missing input handling."""

    monkeypatch.setattr(TargetPipeline, "_get_chembl_release", lambda self: "ChEMBL_TEST")

    captured: dict[str, object] = {}
    original = PipelineBase.read_input_table

    def _spy(self: PipelineBase, **kwargs):
        captured.update(kwargs)
        return original(self, **kwargs)

    monkeypatch.setattr(PipelineBase, "read_input_table", _spy)

    pipeline = TargetPipeline(_target_config, run_id="unit-test")

    result = pipeline.extract()

    assert result.empty
    assert "target_chembl_id" in result.columns
    assert captured["default_filename"] == Path("target.csv")
    assert captured["expected_columns"][0] == "target_chembl_id"
