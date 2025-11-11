"""Unit tests covering the high-level pipeline configuration models."""

from __future__ import annotations

from typing import Any

import pytest

from bioetl.config.models import (
    CacheConfig,
    DeterminismConfig,
    DeterminismSortingConfig,
    HTTPClientConfig,
    HTTPConfig,
    PipelineConfig,
    PipelineMetadata,
)
from bioetl.config.models.source import SourceConfig


def _make_pipeline_config(**overrides: Any) -> PipelineConfig:
    base_kwargs: dict[str, Any] = {
        "version": 1,
        "pipeline": PipelineMetadata(name="activity", version="1.2.3"),
        "http": HTTPConfig(default=HTTPClientConfig()),
    }
    base_kwargs.update(overrides)
    return PipelineConfig(**base_kwargs)


@pytest.mark.unit
def test_pipeline_config_defaults() -> None:
    config = _make_pipeline_config()

    assert config.cache == CacheConfig()
    assert config.paths.input_root == "data/input"
    assert config.paths.output_root == "data/output"
    assert config.cli.limit is None
    assert config.materialization.default_format == "parquet"
    assert config.determinism.enabled is True


@pytest.mark.unit
def test_pipeline_config_requires_schema_when_column_order_set() -> None:
    determinism = DeterminismConfig(column_order=("molecule_id",))

    with pytest.raises(ValueError, match="schema_out"):
        _make_pipeline_config(determinism=determinism)


@pytest.mark.unit
def test_determinism_config_validates_sorting_length() -> None:
    with pytest.raises(ValueError, match="ascending"):
        DeterminismConfig(
            sort=DeterminismSortingConfig(by=["a", "b"], ascending=[True]),
        )


@pytest.mark.unit
def test_determinism_config_rejects_duplicate_columns() -> None:
    with pytest.raises(ValueError, match="duplicate"):
        DeterminismConfig(sort=DeterminismSortingConfig(by=["a", "a"]))


@pytest.mark.unit
def test_pipeline_config_accepts_source_overrides() -> None:
    source_config = SourceConfig(enabled=False, description="disabled for smoke tests")
    config = _make_pipeline_config(sources={"chembl": source_config})

    assert "chembl" in config.sources
    assert config.sources["chembl"].enabled is False
    assert config.sources["chembl"].description == "disabled for smoke tests"

