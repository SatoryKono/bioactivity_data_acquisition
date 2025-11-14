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
    PipelineCommonCompat,
    PipelineConfig,
    PipelineDomainConfig,
    PipelineInfrastructureConfig,
    PipelineMetadata,
    SourceConfig,
)


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
def test_pipeline_config_domain_and_infrastructure_sections() -> None:
    config = _make_pipeline_config()

    assert isinstance(config.domain, PipelineDomainConfig)
    assert isinstance(config.infrastructure, PipelineInfrastructureConfig)
    assert config.domain.validation == config.validation
    assert config.infrastructure.cli == config.cli


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


@pytest.mark.unit
def test_pipeline_config_exposes_expected_sections() -> None:
    config = _make_pipeline_config()

    expected_sections = {
        "domain",
        "infrastructure",
        "runtime",
        "io",
        "http",
        "cache",
        "paths",
        "determinism",
        "materialization",
        "fallbacks",
        "validation",
        "transform",
        "postprocess",
        "logging",
        "telemetry",
        "sources",
        "cli",
        "chembl",
    }

    missing = [section for section in expected_sections if not hasattr(config, section)]
    assert not missing, f"missing sections: {missing}"


@pytest.mark.unit
def test_pipeline_config_model_copy_replaces_sections() -> None:
    config = _make_pipeline_config()

    updated_cli = config.cli.model_copy(update={"limit": 25})
    updated_validation = config.validation.model_copy(update={"strict": False})
    updated_materialization = config.materialization.model_copy(update={"root": "custom"})

    patched = config.model_copy(
        update={
            "cli": updated_cli,
            "validation": updated_validation,
            "materialization": updated_materialization,
        }
    )

    assert patched.cli.limit == 25
    assert config.cli.limit is None

    assert patched.validation.strict is False
    assert config.validation.strict is True

    assert patched.materialization.root == "custom"
    assert config.materialization.root == "data/output"


@pytest.mark.unit
def test_pipeline_common_facade_reflects_cli_section() -> None:
    config = _make_pipeline_config()

    updated = config.model_copy(
        update={"cli": config.cli.model_copy(update={"limit": 9, "dry_run": True})}
    )

    assert isinstance(updated.common, PipelineCommonCompat)
    assert updated.common.limit == 9
    assert updated.common.dry_run is True


@pytest.mark.unit
def test_pipeline_config_supports_nested_section_updates() -> None:
    config = _make_pipeline_config()

    updated = config.model_copy(
        update={
            "domain": {
                "validation": config.validation.model_copy(update={"strict": False}),
            },
            "infrastructure": {
                "logging": config.logging.model_copy(update={"level": "DEBUG"}),
            },
        }
    )

    assert updated.validation.strict is False
    assert updated.logging.level == "DEBUG"

