from bioetl.cli.cli_registry import COMMAND_REGISTRY, CommandConfig
from bioetl.core.pipeline import PipelineBase


def test_command_registry_contains_only_materialized_commands() -> None:
    """All CLI entries must resolve to runnable pipeline configs."""

    assert COMMAND_REGISTRY, "Registry should not be empty"

    for name, factory in COMMAND_REGISTRY.items():
        config = factory()
        assert isinstance(config, CommandConfig), name
        assert issubclass(config.pipeline_class, PipelineBase), name
        if config.default_config_path is not None:
            assert config.default_config_path.exists(), name
