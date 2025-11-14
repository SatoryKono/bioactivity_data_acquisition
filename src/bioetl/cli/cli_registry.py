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

from bioetl.pipelines.base import PipelineBase

__all__ = [
    "CommandConfig",
    "ToolCommandConfig",
    "PipelineCommandSpec",
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
class ToolCommandConfig:
    """Configuration for standalone CLI tools shipped with BioETL."""

    name: str
    description: str
    module: str
    attribute: str = "app"


@dataclass(frozen=True)
class PipelineCommandSpec:
    """Declarative pipeline command specification."""

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


TOOL_COMMANDS: dict[str, ToolCommandConfig] = {
    "audit_docs": ToolCommandConfig(
        name="bioetl-audit-docs",
        description="Run documentation audit and collect reports.",
        module="bioetl.cli.tools.audit_docs",
        attribute="main",
    ),
    "build_vocab_store": ToolCommandConfig(
        name="bioetl-build-vocab-store",
        description="Assemble the aggregated ChEMBL vocabulary and export YAML.",
        module="bioetl.cli.tools.build_vocab_store",
        attribute="main",
    ),
    "dup_finder": ToolCommandConfig(
        name="bioetl-dup-finder",
        description="Detect duplicate and near-duplicate code fragments across the repo.",
        module="bioetl.cli.tools.dup_finder",
        attribute="main",
    ),
    "catalog_code_symbols": ToolCommandConfig(
        name="bioetl-catalog-code-symbols",
        description="Build the code entity catalog and related reports.",
        module="bioetl.cli.tools.catalog_code_symbols",
        attribute="main",
    ),
    "check_comments": ToolCommandConfig(
        name="bioetl-check-comments",
        description="Validate code comments and TODO markers.",
        module="bioetl.cli.tools.check_comments",
        attribute="main",
    ),
    "check_output_artifacts": ToolCommandConfig(
        name="bioetl-check-output-artifacts",
        description="Inspect the data/output directory and flag artifacts.",
        module="bioetl.cli.tools.check_output_artifacts",
        attribute="main",
    ),
    "create_matrix_doc_code": ToolCommandConfig(
        name="bioetl-create-matrix-doc-code",
        description="Generate the Doc<->Code matrix and export artifacts.",
        module="bioetl.cli.tools.create_matrix_doc_code",
        attribute="main",
    ),
    "determinism_check": ToolCommandConfig(
        name="bioetl-determinism-check",
        description="Execute two runs and compare their logs.",
        module="bioetl.cli.tools.determinism_check",
        attribute="main",
    ),
    "doctest_cli": ToolCommandConfig(
        name="bioetl-doctest-cli",
        description="Execute CLI examples and generate a report.",
        module="bioetl.cli.tools.doctest_cli",
        attribute="main",
    ),
    "inventory_docs": ToolCommandConfig(
        name="bioetl-inventory-docs",
        description="Collect a Markdown document inventory and compute hashes.",
        module="bioetl.cli.tools.inventory_docs",
        attribute="main",
    ),
    "link_check": ToolCommandConfig(
        name="bioetl-link-check",
        description="Verify documentation links via lychee.",
        module="bioetl.cli.tools.link_check",
        attribute="main",
    ),
    "remove_type_ignore": ToolCommandConfig(
        name="bioetl-remove-type-ignore",
        description="Remove type ignore directives from source files.",
        module="bioetl.cli.tools.remove_type_ignore",
        attribute="main",
    ),
    "run_test_report": ToolCommandConfig(
        name="bioetl-run-test-report",
        description="Generate pytest and coverage reports with metadata.",
        module="bioetl.cli.tools.run_test_report",
        attribute="main",
    ),
    "schema_guard": ToolCommandConfig(
        name="bioetl-schema-guard",
        description="Validate pipeline configs and the Pandera registry.",
        module="bioetl.cli.tools.schema_guard",
        attribute="main",
    ),
    "semantic_diff": ToolCommandConfig(
        name="bioetl-semantic-diff",
        description="Compare documentation and code to produce a diff.",
        module="bioetl.cli.tools.semantic_diff",
        attribute="main",
    ),
    "vocab_audit": ToolCommandConfig(
        name="bioetl-vocab-audit",
        description="Audit ChEMBL vocabularies and generate a report.",
        module="bioetl.cli.tools.vocab_audit",
        attribute="main",
    ),
    "qc_boundary_check": ToolCommandConfig(
        name="bioetl-qc-boundary-check",
        description=(
            "Static verification that prevents direct or indirect imports of bioetl.qc from the CLI layer."
        ),
        module="bioetl.cli.tools.qc_boundary_check",
        attribute="main",
    ),
}
