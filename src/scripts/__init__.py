"""Compatibility wrapper around the modern ``bioetl.cli.app`` helpers."""

from bioetl.cli.app import (
    PIPELINE_COMMAND_REGISTRY,
    PIPELINE_REGISTRY,
    create_pipeline_app,
    get_pipeline_command_config,
    register_pipeline_command,
)

__all__ = [
    "PIPELINE_COMMAND_REGISTRY",
    "PIPELINE_REGISTRY",
    "create_pipeline_app",
    "get_pipeline_command_config",
    "register_pipeline_command",
]
