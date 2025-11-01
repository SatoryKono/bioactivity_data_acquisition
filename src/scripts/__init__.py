"""Shared registry and helpers for CLI pipeline entrypoints."""

from __future__ import annotations

from dataclasses import replace

import typer

from bioetl.cli.command import PipelineCommandConfig, create_pipeline_command
from bioetl.cli.registry import build_registry, get_command_config

PIPELINE_COMMAND_REGISTRY: dict[str, PipelineCommandConfig] = build_registry()


def get_pipeline_command_config(key: str) -> PipelineCommandConfig:
    try:
        config = PIPELINE_COMMAND_REGISTRY[key]
    except KeyError as exc:  # pragma: no cover - defensive branch
        raise KeyError(f"Unknown pipeline registry key: {key}") from exc
    return replace(config)


def register_pipeline_command(app: typer.Typer, key: str) -> None:
    command = create_pipeline_command(get_command_config(key))
    app.command(name=key)(command)


def create_pipeline_app(key: str, help_text: str) -> typer.Typer:
    """Build a Typer application wired to ``key`` in the pipeline registry."""

    app = typer.Typer(help=help_text)
    register_pipeline_command(app, key)
    return app


__all__ = [
    "PIPELINE_COMMAND_REGISTRY",
    "get_pipeline_command_config",
    "register_pipeline_command",
    "create_pipeline_app",
]
