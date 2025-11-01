"""High-level helpers for building the BioETL CLI applications."""

from __future__ import annotations

from dataclasses import replace
from typing import Any, Mapping

import typer

from bioetl.cli.command import PipelineCommandConfig, create_pipeline_command
from bioetl.cli.registry import build_registry
from bioetl.pipelines.registry import PIPELINE_REGISTRY as PIPELINE_CLASS_REGISTRY

# Build the authoritative mapping of CLI command configurations keyed by their
# public Typer command names.  ``build_registry`` always returns a new mapping so
# we store a defensive copy for reuse across the CLI entrypoints.
PIPELINE_COMMAND_REGISTRY: dict[str, PipelineCommandConfig] = build_registry()


def _resolve_pipeline_key(pipeline_key: str) -> str:
    """Map class-level pipeline identifiers onto CLI command names."""

    overrides = {
        "pubchem_molecule": "pubchem",
        "uniprot_protein": "uniprot",
        "iuphar_target": "gtp_iuphar",
        "semantic_scholar": "semantic_scholar",
    }
    if pipeline_key in overrides:
        return overrides[pipeline_key]
    if "_" in pipeline_key:
        _, suffix = pipeline_key.split("_", 1)
        return suffix
    return pipeline_key


PIPELINE_REGISTRY: dict[str, PipelineCommandConfig] = {}
for pipeline_key in PIPELINE_CLASS_REGISTRY:
    resolved_key = _resolve_pipeline_key(pipeline_key)
    try:
        PIPELINE_REGISTRY[pipeline_key] = PIPELINE_COMMAND_REGISTRY[resolved_key]
    except KeyError as exc:  # pragma: no cover - surfaces misconfigured wiring
        raise KeyError(
            f"No CLI configuration registered for pipeline '{pipeline_key}'"
        ) from exc


def iter_pipeline_commands() -> Mapping[str, PipelineCommandConfig]:
    """Expose a copy of the available Typer command configurations."""

    return PIPELINE_COMMAND_REGISTRY.copy()


def get_pipeline_command_config(key: str) -> PipelineCommandConfig:
    """Return a defensive copy of the CLI configuration registered under ``key``."""

    if key in PIPELINE_COMMAND_REGISTRY:
        return replace(PIPELINE_COMMAND_REGISTRY[key])
    try:
        config = PIPELINE_REGISTRY[key]
    except KeyError as exc:  # pragma: no cover - defensive branch
        raise KeyError(f"Unknown pipeline registry key: {key}") from exc
    return replace(config)


def register_pipeline_command(app: typer.Typer, key: str) -> None:
    """Register the Typer command for ``key`` on ``app``."""

    command = create_pipeline_command(get_pipeline_command_config(key))
    app.command(name=key)(command)


def create_pipeline_app(key: str, help_text: str) -> Any:  # type: ignore[no-any-return]
    """Build a Typer application that exposes the pipeline registered as ``key``."""

    app = typer.Typer(help=help_text)
    register_pipeline_command(app, key)
    return app


__all__ = [
    "PIPELINE_COMMAND_REGISTRY",
    "PIPELINE_REGISTRY",
    "create_pipeline_app",
    "get_pipeline_command_config",
    "iter_pipeline_commands",
    "register_pipeline_command",
]
