"""Static command registry for BioETL CLI.

This module defines the static registry of all available pipeline commands.
Adding a new pipeline requires explicitly adding its configuration to this registry.
"""

from __future__ import annotations

import importlib
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any

__all__ = [
    "CommandConfig",
    "CommandSpec",
    "build_command_config",
    "COMMAND_REGISTRY",
]


@dataclass(frozen=True)
class CommandConfig:
    """Configuration for a pipeline command."""

    name: str
    description: str
    pipeline_class: type[Any]
    default_config_path: Path | None = None


@dataclass(frozen=True)
class CommandSpec:
    """Declarative description of a CLI command for registry construction."""

    name: str
    description: str
    module: str
    class_name: str
    default_config_path: str | Path | None = None
    aliases: tuple[str, ...] = ()


def build_command_config(spec: CommandSpec) -> CommandConfig:
    """Build a :class:`CommandConfig` instance from a command specification."""

    pipeline_module = importlib.import_module(spec.module)
    try:
        pipeline_class = getattr(pipeline_module, spec.class_name)
    except AttributeError as exc:  # pragma: no cover - defensive
        msg = (
            f"Pipeline class '{spec.class_name}' not found in module '{spec.module}'"
        )
        raise ImportError(msg) from exc

    default_config_path = (
        Path(spec.default_config_path)
        if spec.default_config_path is not None
        else None
    )

    return CommandConfig(
        name=spec.name,
        description=spec.description,
        pipeline_class=pipeline_class,
        default_config_path=default_config_path,
    )


def _build_registry(specs: Sequence[CommandSpec]) -> dict[str, Callable[[], CommandConfig]]:
    """Construct the command registry mapping from command specifications."""

    registry: dict[str, Callable[[], CommandConfig]] = {}
    for spec in specs:
        builder = partial(build_command_config, spec)
        registry[spec.name] = builder
        for alias in spec.aliases:
            registry[alias] = builder
    return registry


COMMAND_SPECS: tuple[CommandSpec, ...] = (
    CommandSpec(
        name="activity_chembl",
        aliases=("activity",),
        description="Extract biological activity records from ChEMBL API and normalize them to the project schema.",
        module="bioetl.pipelines.activity.activity",
        class_name="ChemblActivityPipeline",
        default_config_path=Path("configs/pipelines/activity/activity_chembl.yaml"),
    ),
    CommandSpec(
        name="assay_chembl",
        aliases=("assay",),
        description="Extract assay records from ChEMBL API.",
        module="bioetl.pipelines.assay.assay",
        class_name="ChemblAssayPipeline",
        default_config_path=Path("configs/pipelines/assay/assay_chembl.yaml"),
    ),
    CommandSpec(
        name="target",
        aliases=("target_chembl",),
        description="Extract target records from ChEMBL API and normalize them to the project schema.",
        module="bioetl.pipelines.target.target",
        class_name="ChemblTargetPipeline",
        default_config_path=Path("configs/pipelines/target/target_chembl.yaml"),
    ),
    CommandSpec(
        name="document",
        aliases=("document_chembl",),
        description="Extract document records from ChEMBL API and normalize them to the project schema.",
        module="bioetl.pipelines.document.document",
        class_name="ChemblDocumentPipeline",
        default_config_path=Path("configs/pipelines/document/document_chembl.yaml"),
    ),
    CommandSpec(
        name="testitem_chembl",
        aliases=("testitem",),
        description="Extract molecule records from ChEMBL API and normalize them to test items.",
        module="bioetl.pipelines.testitem.testitem",
        class_name="TestItemChemblPipeline",
        default_config_path=Path("configs/pipelines/testitem/testitem_chembl.yaml"),
    ),
)


# Static registry mapping command names to their build functions
COMMAND_REGISTRY: dict[str, Callable[[], CommandConfig]] = _build_registry(COMMAND_SPECS)
