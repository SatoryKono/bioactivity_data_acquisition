"""Unit tests for :mod:`bioetl.config.models.PipelineConfig`."""

from __future__ import annotations

from pathlib import Path

import pytest

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

