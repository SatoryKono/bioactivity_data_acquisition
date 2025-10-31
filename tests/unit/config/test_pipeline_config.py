"""Unit tests for :mod:`bioetl.config.models.PipelineConfig`."""

from __future__ import annotations

from pathlib import Path

import pytest

from bioetl.config import load_config
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
        "pipeline": {"name": "test", "entity": "unit", "version": "1.0.0"},
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


def test_pipeline_config_rejects_invalid_pipeline_version(tmp_path: Path) -> None:
    """Pipeline version strings must be valid semantic versions."""

    payload = _base_config(tmp_path)
    payload["pipeline"]["version"] = "invalid version"

    with pytest.raises(ValueError, match="Invalid pipeline version"):
        PipelineConfig.model_validate(payload)


def test_pipeline_config_normalises_validation_suffixes(tmp_path: Path) -> None:
    """Column validation ignore suffixes should be lower-cased and deduplicated."""

    payload = _base_config(tmp_path)
    payload["determinism"] = {
        "column_validation_ignore_suffixes": [
            "  _QC.csv  ",
            "_Qc.CsV",
            "_Custom_Report.CSV",
        ]
    }

    config = PipelineConfig.model_validate(payload)

    assert config.determinism.column_validation_ignore_suffixes == [
        "_qc.csv",
        "_custom_report.csv",
    ]


def test_pipeline_config_rejects_unknown_fallback_strategy(tmp_path: Path) -> None:
    """Unsupported fallback strategies should be rejected during validation."""

    payload = _base_config(tmp_path)
    payload["fallbacks"] = {"strategies": ["cache", "bogus"]}

    with pytest.raises(ValueError, match="Unknown fallback strategy 'bogus'"):
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


def test_pipeline_config_supports_manager_fallbacks(tmp_path: Path) -> None:
    """Network/timeout/5xx strategies should be accepted and normalized."""

    payload = _base_config(tmp_path)
    payload["fallbacks"] = {"strategies": ["network", "timeout", "5xx", "network"]}

    config = PipelineConfig.model_validate(payload)

    assert config.fallbacks.strategies == ["network", "timeout", "5xx"]


def test_materialization_resolves_relative_dataset_paths(tmp_path: Path) -> None:
    """Materialization config should combine root, subdir, and filenames into paths."""

    payload = _base_config(tmp_path)
    payload["materialization"] = {
        "root": str(tmp_path / "output"),
        "pipeline_subdir": "demo",
        "stages": {
            "gold": {
                "datasets": {
                    "targets": {"filename": "targets_final"},
                }
            }
        },
    }

    config = PipelineConfig.model_validate(payload)

    resolved = config.materialization.resolve_dataset_path("gold", "targets", "parquet")
    assert resolved == (tmp_path / "output" / "demo" / "targets_final.parquet")
    assert config.materialization.infer_dataset_format("gold", "targets") == "parquet"


@pytest.mark.parametrize(
    "pipeline_name",
    ["activity", "assay", "document", "target", "testitem"],
)
def test_pipeline_configs_expose_materialization_paths(
    pipeline_name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """All pipeline configs should expose resolvable materialization datasets."""

    monkeypatch.setenv("IUPHAR_API_KEY", "test-key")
    config = load_config(f"configs/pipelines/{pipeline_name}.yaml")
    materialization = config.materialization

    for stage_name, stage in materialization.stages.items():
        for dataset_name in stage.datasets:
            default_format = materialization.infer_dataset_format(stage_name, dataset_name)
            if default_format is None:
                continue
            resolved = materialization.resolve_dataset_path(stage_name, dataset_name, default_format)
            assert resolved is not None
            assert resolved.suffix

