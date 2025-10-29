"""Unit tests for shared schema validation helper."""

from __future__ import annotations

import types
import sys
from importlib import import_module
from pathlib import Path

import pandas as pd
import pandera as pa
import pytest
from pandera.errors import SchemaErrors

import bioetl  # noqa: F401  # Ensure base package is loaded

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_PIPELINES_PATH = _PROJECT_ROOT / "src" / "bioetl" / "pipelines"
_CORE_PATH = _PROJECT_ROOT / "src" / "bioetl" / "core"
if "bioetl.pipelines" not in sys.modules:
    stub = types.ModuleType("bioetl.pipelines")
    stub.__path__ = [str(_PIPELINES_PATH)]
    sys.modules["bioetl.pipelines"] = stub

if "bioetl.core" not in sys.modules:
    core_stub = types.ModuleType("bioetl.core")
    core_stub.__path__ = [str(_CORE_PATH)]
    sys.modules["bioetl.core"] = core_stub

PipelineBase = import_module("bioetl.pipelines.base").PipelineBase


class _DummyPipeline(PipelineBase):
    """Minimal pipeline implementation for testing base helpers."""

    def extract(self, *args, **kwargs):  # type: ignore[override]
        raise NotImplementedError

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:  # type: ignore[override]
        return df


def _make_pipeline(severity_threshold: str) -> _DummyPipeline:
    config = types.SimpleNamespace(
        pipeline=types.SimpleNamespace(name="dummy"),
        qc=types.SimpleNamespace(severity_threshold=severity_threshold),
    )
    return _DummyPipeline(config, "unit-test")


@pytest.mark.parametrize("severity", ["error", "critical"])
def test_validate_with_schema_raises_above_threshold(severity: str) -> None:
    """Severity above the configured threshold should raise SchemaErrors."""

    pipeline = _make_pipeline(severity_threshold="warning")
    schema = pa.DataFrameSchema({"value": pa.Column(int)})
    frame = pd.DataFrame({"value": ["oops"]})
    failure_flags: list[bool] = []

    def _failure_handler(exc: SchemaErrors, should_fail: bool) -> None:
        assert isinstance(exc, SchemaErrors)
        failure_flags.append(should_fail)

    with pytest.raises(SchemaErrors):
        pipeline._validate_with_schema(
            frame,
            schema,
            dataset_name="dummy",
            severity=severity,
            failure_handler=_failure_handler,
        )

    summary = pipeline.qc_summary_data["validation"]["dummy"]
    assert summary["status"] == "failed"
    assert summary["severity"] == severity
    issue = pipeline.validation_issues[-1]
    assert issue["metric"] == "schema.dummy"
    assert issue["status"] == "failed"
    assert issue["severity"] == severity
    assert failure_flags == [True]


def test_validate_with_schema_records_issue_below_threshold() -> None:
    """When severity is below threshold the failure is recorded without raising."""

    pipeline = _make_pipeline(severity_threshold="critical")
    schema = pa.DataFrameSchema({"value": pa.Column(int)})
    frame = pd.DataFrame({"value": ["oops"]})
    failure_flags: list[bool] = []

    def _failure_handler(exc: SchemaErrors, should_fail: bool) -> None:
        assert isinstance(exc, SchemaErrors)
        failure_flags.append(should_fail)

    result = pipeline._validate_with_schema(
        frame,
        schema,
        dataset_name="dummy",
        severity="error",
        failure_handler=_failure_handler,
    )

    assert result is frame
    summary = pipeline.qc_summary_data["validation"]["dummy"]
    assert summary["status"] == "failed"
    assert summary["severity"] == "error"
    issue = pipeline.validation_issues[-1]
    assert issue["status"] == "failed"
    assert issue["severity"] == "error"
    assert issue["metric"] == "schema.dummy"
    assert failure_flags == [False]

