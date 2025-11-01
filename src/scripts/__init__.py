"""Shared registry and helpers for CLI pipeline entrypoints."""

from __future__ import annotations

from dataclasses import replace
import typer

from bioetl.cli.command import PipelineCommandConfig, create_pipeline_command
from bioetl.cli.registry import build_registry, get_command_config
from bioetl.pipelines.registry import PIPELINE_REGISTRY as PIPELINE_CLASS_REGISTRY

PIPELINE_COMMAND_REGISTRY: dict[str, PipelineCommandConfig] = build_registry()


def _resolve_legacy_key(pipeline_key: str) -> str:
    overrides = {
        "pubchem_molecule": "pubchem",
        "uniprot_protein": "uniprot",
        "iuphar_target": "gtp_iuphar",
    }
    if pipeline_key in overrides:
        return overrides[pipeline_key]
    return pipeline_key.split("_", 1)[1]


PIPELINE_REGISTRY: dict[str, PipelineCommandConfig] = {}
for pipeline_key in PIPELINE_CLASS_REGISTRY:
    legacy_key = _resolve_legacy_key(pipeline_key)
    try:
        PIPELINE_REGISTRY[pipeline_key] = PIPELINE_COMMAND_REGISTRY[legacy_key]
    except KeyError as exc:  # pragma: no cover - defensive to surface wiring errors early
        raise KeyError(
            f"No CLI configuration registered for pipeline '{pipeline_key}'"
        ) from exc


def get_pipeline_command_config(key: str) -> PipelineCommandConfig:
    try:
        config = PIPELINE_COMMAND_REGISTRY[key]
    except KeyError:  # pragma: no cover - defensive branch
        try:
            config = PIPELINE_REGISTRY[key]
        except KeyError as inner_exc:  # pragma: no cover - defensive branch
            raise KeyError(f"Unknown pipeline registry key: {key}") from inner_exc
        else:
            return replace(config)
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
    "PIPELINE_REGISTRY",
    "get_pipeline_command_config",
    "register_pipeline_command",
    "create_pipeline_app",
]
