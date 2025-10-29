"""Tests for shared schema validation helpers in PipelineBase."""

from __future__ import annotations

from pathlib import Path
import sys
import types

import pandas as pd
import pandera as pa
import pytest
from pandera.errors import SchemaErrors

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "src"))

if "cachetools" not in sys.modules:
    cachetools_stub = types.ModuleType("cachetools")

    class _DummyTTLCache(dict):  # noqa: D401 - minimal stub for tests
        def __init__(self, maxsize, ttl):
            super().__init__()
            self.maxsize = maxsize
            self.ttl = ttl

    cachetools_stub.TTLCache = _DummyTTLCache  # type: ignore[attr-defined]
    sys.modules["cachetools"] = cachetools_stub

from bioetl.config.loader import load_config
from bioetl.pipelines.base import PipelineBase


class _DummyPipeline(PipelineBase):
    """Minimal pipeline implementation for testing base validation logic."""

    def extract(self, *args, **kwargs):  # noqa: D401 - unused in tests
        raise NotImplementedError

    def transform(self, df: pd.DataFrame):  # noqa: D401 - unused in tests
        return df


@pytest.fixture()
def pipeline_config(tmp_path: Path):
    """Provide a pipeline config with isolated output paths."""

    config = load_config("configs/pipelines/activity.yaml")
    # Ensure output writer points at temporary directory to avoid pollution.
    config.paths.output_root = tmp_path
    return config


@pytest.fixture()
def dummy_pipeline(pipeline_config):
    """Instantiate a dummy pipeline for validation tests."""

    pipeline_config.qc.thresholds = {}
    return _DummyPipeline(pipeline_config, run_id="test-run")


def test_validate_with_schema_records_issue_below_threshold(dummy_pipeline):
    """Schema violations below the severity threshold should be recorded, not raised."""

    dummy_pipeline.config.qc.severity_threshold = "critical"
    schema = pa.DataFrameSchema({"id": pa.Column(int)})
    df = pd.DataFrame({"value": [1, 2, 3]})

    result = dummy_pipeline._validate_with_schema(
        df,
        schema,
        "dummy_dataset",
        severity="error",
        metric_name="dummy",
    )

    # Validation should return the original dataframe without raising.
    pd.testing.assert_frame_equal(result, df)

    summary = dummy_pipeline.qc_summary_data.get("validation", {}).get("dummy_dataset")
    assert summary == {"status": "failed", "errors": 1}

    # Ensure a QC issue has been recorded with the expected severity.
    issue = dummy_pipeline.validation_issues[-1]
    assert issue["metric"] == "schema.dummy"
    assert issue["severity"] == "error"
    assert issue["status"] == "failed"


def test_validate_with_schema_raises_at_threshold(dummy_pipeline):
    """Schema violations at or above the severity threshold should raise errors."""

    dummy_pipeline.config.qc.severity_threshold = "warning"
    schema = pa.DataFrameSchema({"id": pa.Column(int)})
    df = pd.DataFrame({"value": [1, 2]})

    with pytest.raises(SchemaErrors):
        dummy_pipeline._validate_with_schema(
            df,
            schema,
            "dummy_dataset",
            severity="error",
            metric_name="dummy",
        )

    issue = dummy_pipeline.validation_issues[-1]
    assert issue["metric"] == "schema.dummy"
    assert issue["status"] == "failed"
    assert issue["severity"] == "error"
