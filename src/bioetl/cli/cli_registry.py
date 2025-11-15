"""Static command registry for BioETL CLI.

This module defines the static registry of all available pipeline commands.
Adding a new pipeline requires explicitly adding its configuration to this registry.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any

from bioetl.cli.tool_specs import ToolCommandSpec, TOOL_COMMAND_SPECS
from bioetl.core.pipeline import PipelineBase

__all__ = [
    "CommandConfig",
    "PipelineCommandSpec",
    "ToolCommandConfig",
    "PIPELINE_REGISTRY",
    "COMMAND_REGISTRY",
    "TOOL_COMMANDS",
]


@dataclass(frozen=True)
class CommandConfig:
    """Configuration for a pipeline command."""

    name: str
    description: str
    pipeline_class: type[Any]
    default_config_path: Path | None = None
    canonical_name: str | None = None


@dataclass(frozen=True)
class PipelineCommandSpec:
    """Declarative pipeline command specification."""
@dataclass(frozen=True)
class ToolCommandConfig:
    """Configuration for CLI utility commands declared in ``TOOL_COMMAND_SPECS``."""

    name: str
    module: str
    attribute: str
    description: str

    code: str
    description: str
    pipeline_path: str | None
    default_config: str | None = None
    aliases: tuple[str, ...] = ()
    not_implemented_message: str | None = None


def _build_config(
    *,
    command_name: str,
    description: str,
    pipeline_path: str,
    default_config: str | None,
    canonical_name: str,
) -> CommandConfig:
    """Resolve the pipeline class and construct a command configuration."""
    pipeline_class = _load_pipeline_class(pipeline_path)
    default_path = Path(default_config) if default_config is not None else None
    return CommandConfig(
        name=command_name,
        description=description,
        pipeline_class=pipeline_class,
        default_config_path=default_path,
        canonical_name=canonical_name,
    )


def _load_pipeline_class(path: str) -> type[Any]:
    """Import and return the pipeline class referenced by the dotted path."""
    module_path, class_name = path.rsplit(".", 1)
    module = import_module(module_path)
    pipeline_cls = getattr(module, class_name)
    if not isinstance(pipeline_cls, type):
        msg = f"Object '{class_name}' from '{module_path}' is not a class."
        raise TypeError(msg)
    if not issubclass(pipeline_cls, PipelineBase):
        msg = f"Class '{class_name}' from '{module_path}' is not a PipelineBase subclass."
        raise TypeError(msg)
    return pipeline_cls


def _create_command_registry(
    specs: Iterable[PipelineCommandSpec],
) -> dict[str, Callable[[], CommandConfig]]:
    registry: dict[str, Callable[[], CommandConfig]] = {}
    for spec in specs:
        names = (spec.code, *spec.aliases)
        for command_name in names:
            registry[command_name] = _make_config_factory(spec, command_name=command_name)
    return registry


def _make_config_factory(
    spec: PipelineCommandSpec,
    *,
    command_name: str,
) -> Callable[[], CommandConfig]:
    pipeline_path = spec.pipeline_path
    if pipeline_path is None:
        message = spec.not_implemented_message or f"{spec.code} pipeline not yet implemented"

        def _not_implemented() -> CommandConfig:
            raise NotImplementedError(message)

        return _not_implemented

    def _factory() -> CommandConfig:
        return _build_config(
            command_name=command_name,
            description=spec.description,
            pipeline_path=pipeline_path,
            default_config=spec.default_config,
            canonical_name=spec.code,
        )

    return _factory


PIPELINE_REGISTRY: tuple[PipelineCommandSpec, ...] = (
    PipelineCommandSpec(
        code="activity_chembl",
        description=(
            "Extract biological activity records from ChEMBL API and normalize them to the project schema."
        ),
        pipeline_path="bioetl.pipelines.chembl.activity.run.ChemblActivityPipeline",
        default_config="configs/pipelines/activity/activity_chembl.yaml",
    ),
    PipelineCommandSpec(
        code="assay_chembl",
        description="Extract assay records from ChEMBL API.",
        pipeline_path="bioetl.pipelines.chembl.assay.run.ChemblAssayPipeline",
        default_config="configs/pipelines/assay/assay_chembl.yaml",
    ),
    PipelineCommandSpec(
        code="testitem_chembl",
        description="Extract molecule records from ChEMBL API and normalize them to test items.",
        pipeline_path="bioetl.pipelines.chembl.testitem.run.TestItemChemblPipeline",
        default_config="configs/pipelines/testitem/testitem_chembl.yaml",
    ),
    PipelineCommandSpec(
        code="target_chembl",
        description="Extract target records from ChEMBL API and normalize them to the project schema.",
        pipeline_path="bioetl.pipelines.chembl.target.run.ChemblTargetPipeline",
        default_config="configs/pipelines/target/target_chembl.yaml",
    ),
    PipelineCommandSpec(
        code="document_chembl",
        description=(
            "Extract document records from ChEMBL API and normalize them to the project schema."
        ),
        pipeline_path="bioetl.pipelines.chembl.document.run.ChemblDocumentPipeline",
        default_config="configs/pipelines/document/document_chembl.yaml",
    ),
    PipelineCommandSpec(
        code="pubchem",
        description="Extract compound data from PubChem and normalize to the project schema.",
        pipeline_path=None,
        not_implemented_message="PubChem pipeline not yet implemented",
    ),
    PipelineCommandSpec(
        code="uniprot",
        description="Extract protein records from UniProt and normalize to the project schema.",
        pipeline_path=None,
        not_implemented_message="UniProt pipeline not yet implemented",
    ),
    PipelineCommandSpec(
        code="gtp_iuphar",
        description="Extract ligand and target data from IUPHAR and normalize to the project schema.",
        pipeline_path=None,
        not_implemented_message="IUPHAR pipeline not yet implemented",
    ),
    PipelineCommandSpec(
        code="openalex",
        description="Extract scholarly metadata from OpenAlex and normalize to the project schema.",
        pipeline_path=None,
        not_implemented_message="OpenAlex pipeline not yet implemented",
    ),
    PipelineCommandSpec(
        code="crossref",
        description="Extract bibliographic metadata from Crossref and normalize to the project schema.",
        pipeline_path=None,
        not_implemented_message="Crossref pipeline not yet implemented",
    ),
    PipelineCommandSpec(
        code="pubmed",
        description="Extract publication data from PubMed and normalize to the project schema.",
        pipeline_path=None,
        not_implemented_message="PubMed pipeline not yet implemented",
    ),
    PipelineCommandSpec(
        code="semantic_scholar",
        description="Extract publication data from Semantic Scholar and normalize to the project schema.",
        pipeline_path=None,
        not_implemented_message="Semantic Scholar pipeline not yet implemented",
    ),
)


COMMAND_REGISTRY: dict[str, Callable[[], CommandConfig]] = _create_command_registry(
    PIPELINE_REGISTRY
)


def _build_tool_commands(
    specs: Iterable[ToolCommandSpec],
) -> dict[str, ToolCommandConfig]:
    commands: dict[str, ToolCommandConfig] = {}
    for spec in specs:
        commands[spec.code] = ToolCommandConfig(
            name=spec.script_name,
            module=spec.alias_module,
            attribute=spec.attribute,
            description=spec.description,
        )
    return commands


TOOL_COMMANDS: dict[str, ToolCommandConfig] = _build_tool_commands(TOOL_COMMAND_SPECS)
