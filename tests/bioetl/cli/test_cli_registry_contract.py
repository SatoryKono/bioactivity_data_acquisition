from __future__ import annotations

from pathlib import Path

import pytest

from bioetl.cli.cli_registry import (
    COMMAND_REGISTRY,
    PIPELINE_REGISTRY,
    TOOL_COMMANDS,
    CommandConfig,
)
from bioetl.cli.tool_specs import TOOL_COMMAND_SPECS
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
    expected_modules = {
        spec.code: (spec.script_name, spec.alias_module, spec.description)
        for spec in TOOL_COMMAND_SPECS
    }
    assert set(TOOL_COMMANDS) == set(expected_modules)
    for key, (script_name, module_path, description) in expected_modules.items():
        tool_config = TOOL_COMMANDS[key]
        assert tool_config.name == script_name
        assert tool_config.module == module_path
        assert tool_config.attribute == "main"
        assert tool_config.description == description


