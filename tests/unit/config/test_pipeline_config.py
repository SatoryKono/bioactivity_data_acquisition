"""Unit tests for :mod:`bioetl.config.models.PipelineConfig`."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from bioetl.config.loader import load_config
from bioetl.config.models import PipelineConfig


def _base_config(tmp_path: Path) -> dict:
    """Construct a minimal configuration payload for validation tests."""

    cache_dir = tmp_path / "cache"
    input_root = tmp_path / "input"
    output_root = tmp_path / "output"

    cache_dir.mkdir()
    input_root.mkdir()
    output_root.mkdir()

    return {
        "version": 1,
        "pipeline": {"name": "test", "entity": "unit"},
        "http": {},
        "cache": {"enabled": False, "directory": str(cache_dir), "ttl": 0},
        "paths": {"input_root": str(input_root), "output_root": str(output_root)},
        "determinism": {},
        "sources": {
            "chembl": {
                "base_url": "https://chembl.example/api",
            }
        },
    }


def test_pipeline_config_assigns_default_batch_size(tmp_path: Path) -> None:
    """Missing ChEMBL batch size should default to the API-safe limit."""

    payload = _base_config(tmp_path)
    config = PipelineConfig.model_validate(payload)

    assert config.sources["chembl"].batch_size == 25


def test_pipeline_config_allows_batch_size_within_limit(tmp_path: Path) -> None:
    """Explicit ChEMBL batch sizes within limits should be preserved."""

    payload = _base_config(tmp_path)
    payload["sources"]["chembl"]["batch_size"] = 10

    config = PipelineConfig.model_validate(payload)

    assert config.sources["chembl"].batch_size == 10


def test_pipeline_config_rejects_batch_size_exceeding_limit(tmp_path: Path) -> None:
    """Batch sizes above the API limit should raise a validation error."""

    payload = _base_config(tmp_path)
    payload["sources"]["chembl"]["batch_size"] = 30

    with pytest.raises(ValueError, match="batch_size must be <= 25"):
        PipelineConfig.model_validate(payload)


def test_pipeline_config_rejects_unknown_fallback_strategy(tmp_path: Path) -> None:
    """Unsupported fallback strategies should be rejected during validation."""

    payload = _base_config(tmp_path)
    payload["fallbacks"] = {"strategies": ["cache", "network"]}

    with pytest.raises(ValueError, match="Unknown fallback strategy 'network'"):
        PipelineConfig.model_validate(payload)


def test_pipeline_config_normalizes_fallback_strategies(tmp_path: Path) -> None:
    """Global and source-level fallback strategies should be de-duplicated."""

    payload = _base_config(tmp_path)
    payload["fallbacks"] = {"strategies": ["cache", "partial_retry", "cache"]}
    payload["sources"]["chembl"]["fallback_strategies"] = [
        "partial_retry",
        "cache",
        "partial_retry",
    ]

    config = PipelineConfig.model_validate(payload)

    assert config.fallbacks.strategies == ["cache", "partial_retry"]
    assert config.sources["chembl"].fallback_strategies == ["partial_retry", "cache"]


def test_materialization_defaults_apply_to_dataset(tmp_path: Path) -> None:
    """Materialization defaults should provide formatted filenames and paths."""

    payload = _base_config(tmp_path)
    payload["materialization"] = {
        "defaults": {"root": "{entity}"},
        "stages": {
            "gold": {"format": "parquet", "filename": "{dataset}_final.parquet"},
            "silver": {"format": "parquet", "filename": "{dataset}_silver.parquet"},
        },
        "datasets": {"unit": {}},
    }

    config = PipelineConfig.model_validate(payload)

    silver_stage = config.materialization.get_stage(
        "silver", dataset="unit", pipeline=config.pipeline
    )
    assert silver_stage is not None
    assert silver_stage.format == "parquet"

    silver_path = config.resolve_materialization_path("silver", dataset="unit")
    assert silver_path == Path(tmp_path / "output" / "unit" / "unit_silver.parquet")

    gold_path = config.resolve_materialization_path("gold", dataset="unit")
    assert gold_path == Path(tmp_path / "output" / "unit" / "unit_final.parquet")


@pytest.mark.parametrize(
    ("config_file", "dataset"),
    [
        ("activity.yaml", "activity"),
        ("assay.yaml", "assay"),
        ("document.yaml", "document"),
        ("target.yaml", "targets"),
        ("testitem.yaml", "testitem"),
    ],
)
def test_pipeline_configs_expose_materialization_paths(
    config_file: str, dataset: str
) -> None:
    """Each pipeline config should expose resolved materialization paths."""

    os.environ.setdefault("IUPHAR_API_KEY", "test-key")
    config = load_config(Path("configs/pipelines") / config_file)

    silver_stage = config.materialization.get_stage(
        "silver", dataset=dataset, pipeline=config.pipeline
    )
    assert silver_stage is not None
    assert silver_stage.format == "parquet"

    silver_path = config.resolve_materialization_path("silver", dataset=dataset)
    assert silver_path is not None
    assert silver_path.suffix == ".parquet"

    gold_path = config.resolve_materialization_path("gold", dataset=dataset)
    assert gold_path is not None
    assert gold_path.suffix == ".parquet"


def test_target_pipeline_checkpoint_resolution() -> None:
    """Target pipeline should resolve dataset-specific checkpoint paths."""

    os.environ.setdefault("IUPHAR_API_KEY", "test-key")
    config = load_config(Path("configs/pipelines/target.yaml"))

    checkpoint = config.resolve_checkpoint_path("chembl", dataset="targets")
    assert checkpoint == Path(
        "data/output/target/checkpoints/chembl_targets.json"
    )

    qc_stage = config.materialization.get_stage(
        "qc_report", dataset="targets", pipeline=config.pipeline
    )
    assert qc_stage is not None
    assert qc_stage.format == "csv"

