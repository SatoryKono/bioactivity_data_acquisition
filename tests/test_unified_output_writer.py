from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pandera as pa
import pytest
import yaml

from bioetl.config.models import PipelineConfig
from bioetl.core.io.artifacts import (
    DeterminismSettings,
    RunArtifacts,
    SchemaRegistry,
    SchemaRegistryEntry,
)
from bioetl.core.io.output import UnifiedOutputWriter, validate_with_schema
from bioetl.core.logging import UnifiedLogger


def _build_registry(sort_by: tuple[str, ...] | None = None) -> SchemaRegistry:
    schema = pa.DataFrameSchema(
        {
            "a": pa.Column(pa.Int64, nullable=False),
            "b": pa.Column(pa.Int64, nullable=False),
        },
        ordered=True,
    )
    registry = SchemaRegistry()
    registry.register(
        SchemaRegistryEntry(
            identifier="test.pipeline",
            schema=schema,
            version="1",
            column_order=("a", "b"),
            determinism=DeterminismSettings(sort_by=sort_by),
            business_key_fields=("a",),
            row_hash_fields=("a", "b"),
        )
    )
    return registry


def test_write_dataset_atomic_preserves_column_order_and_sort(tmp_path: Path):
    registry = _build_registry(sort_by=("b",))
    config = PipelineConfig(
        name="test.pipeline",
        metadata={"determinism": {"sort": {"by": ["b"]}}},
    )
    writer = UnifiedOutputWriter(
        output_dir=tmp_path,
        pipeline_code="test.pipeline",
        run_stem="run1",
        schema_registry=registry,
        config=config,
        logger=UnifiedLogger.get("test"),
    )
    df = pd.DataFrame({"b": [2, 1], "a": [10, 20]})
    artifacts = RunArtifacts(output_dir=tmp_path, logs_directory=tmp_path / "logs")

    result = writer.write_dataset_atomic(df, artifacts, format="csv")

    written = pd.read_csv(result.artifacts.data_path)
    assert list(written.columns) == ["a", "b"]
    assert written.to_dict(orient="list") == {"a": [20, 10], "b": [1, 2]}


def test_validate_with_schema_fail_and_allow_modes(tmp_path: Path):
    registry = _build_registry()
    entry = registry.get("test.pipeline")
    df = pd.DataFrame({"a": ["oops"], "b": [1]})
    logger = UnifiedLogger.get("test")

    with pytest.raises(pa.errors.SchemaError):
        validate_with_schema(df, entry, fail_on_schema_drift=True, logger=logger)

    result = validate_with_schema(df, entry, fail_on_schema_drift=False, logger=logger)
    pd.testing.assert_frame_equal(result, df)


def test_write_dataset_atomic_generates_meta_and_manifest(tmp_path: Path):
    registry = _build_registry(sort_by=("b",))
    config = PipelineConfig(
        name="test.pipeline",
        metadata={
            "chembl_release": "33",
            "pipeline_version": "1.0.0",
            "determinism": {"sort": {"by": ["b"]}},
        },
    )
    writer = UnifiedOutputWriter(
        output_dir=tmp_path,
        pipeline_code="test.pipeline",
        run_stem="run1",
        schema_registry=registry,
        config=config,
        logger=UnifiedLogger.get("test"),
    )
    df = pd.DataFrame({"b": [1, 2], "a": [10, 20]})
    artifacts = RunArtifacts(output_dir=tmp_path, logs_directory=tmp_path / "logs")

    result = writer.write_dataset_atomic(df, artifacts, format="csv")

    meta_path = tmp_path / "meta.yaml"
    manifest_path = tmp_path / "run_manifest.json"

    assert meta_path.exists()
    assert manifest_path.exists()

    meta = yaml.safe_load(meta_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert meta["chembl_release"] == "33"
    assert meta["pipeline_version"] == "1.0.0"
    assert meta["rows"] == 2
    assert result.artifacts.data_path.name in meta["artifact_checksums"]

    dataset_manifest = manifest["artifacts"]["dataset"]
    assert dataset_manifest["path"] == result.artifacts.data_path.name
    assert dataset_manifest["hash"]
    assert dataset_manifest["rows"] == 2
