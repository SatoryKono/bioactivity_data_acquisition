from __future__ import annotations

from pathlib import Path

import pytest

from bioetl.cli.cli_registry import (
    COMMAND_REGISTRY,
    PIPELINE_REGISTRY,
    TOOL_COMMANDS,
    CommandConfig,
)
from bioetl.pipelines.base import PipelineBase


def test_pipeline_registry_factories() -> None:
    for spec in PIPELINE_REGISTRY:
        factory = COMMAND_REGISTRY[spec.code]
        if spec.pipeline_path is None:
            with pytest.raises(NotImplementedError):
                factory()
            continue

        config = factory()
        assert isinstance(config, CommandConfig)
        assert config.name == spec.code
        assert config.canonical_name == spec.code
        assert config.description == spec.description
        assert issubclass(config.pipeline_class, PipelineBase)
        if spec.default_config is None:
            assert config.default_config_path is None
        else:
            assert isinstance(config.default_config_path, Path)
            assert config.default_config_path.as_posix() == spec.default_config


def test_tool_command_registry_metadata() -> None:
    assert "qc_boundary_check" in TOOL_COMMANDS
    tool_config = TOOL_COMMANDS["qc_boundary_check"]
    assert tool_config.name == "bioetl-qc-boundary-check"
    assert tool_config.module == "bioetl.cli.tools.qc_boundary_check"
    assert tool_config.attribute == "main"


