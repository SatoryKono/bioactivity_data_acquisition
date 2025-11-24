from __future__ import annotations

from pathlib import Path

from bioetl.cli.cli_registry import COMMAND_REGISTRY, PIPELINE_REGISTRY, CommandConfig
from bioetl.pipelines.unified_base import UnifiedPipelineBase


def test_pipeline_registry_factories() -> None:
    for spec in PIPELINE_REGISTRY:
        factory = COMMAND_REGISTRY[spec.code]

        config = factory()
        assert isinstance(config, CommandConfig)
        assert config.name == spec.code
        assert config.canonical_name == spec.code
        assert config.description == spec.description
        assert issubclass(config.pipeline_class, UnifiedPipelineBase)
        if spec.default_config is None:
            assert config.default_config_path is None
        else:
            assert isinstance(config.default_config_path, Path)
            assert config.default_config_path.as_posix() == spec.default_config
