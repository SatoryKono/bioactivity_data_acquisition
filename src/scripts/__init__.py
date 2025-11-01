"""Shared registry and helpers for CLI pipeline entrypoints."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import typer

from bioetl.cli.command import PipelineCommandConfig, create_pipeline_command
from bioetl.config.paths import get_config_path
from bioetl.pipelines.document import (
    DEFAULT_DOCUMENT_PIPELINE_MODE,
    DOCUMENT_PIPELINE_MODES,
    DocumentPipeline,
)
from bioetl.sources.chembl.activity.pipeline import ActivityPipeline
from bioetl.sources.chembl.assay.pipeline import AssayPipeline
from bioetl.sources.chembl.target.pipeline import TargetPipeline
from bioetl.sources.chembl.testitem.pipeline import TestItemPipeline
from bioetl.sources.iuphar.pipeline import GtpIupharPipeline
from bioetl.sources.pubchem.pipeline import PubChemPipeline
from bioetl.sources.uniprot.pipeline import UniProtPipeline

PIPELINE_COMMAND_REGISTRY: dict[str, PipelineCommandConfig] = {
    "activity": PipelineCommandConfig(
        pipeline_name="activity",
        pipeline_factory=lambda: ActivityPipeline,
        default_config=get_config_path("pipelines/activity.yaml"),
        default_input=Path("data/input/activity.csv"),
        default_output_dir=Path("data/output/activity"),
        description="ChEMBL activity data",
    ),
    "pubchem": PipelineCommandConfig(
        pipeline_name="pubchem",
        pipeline_factory=lambda: PubChemPipeline,
        default_config=get_config_path("pipelines/pubchem.yaml"),
        default_input=Path("data/input/pubchem_lookup.csv"),
        default_output_dir=Path("data/output/pubchem"),
        description="Standalone PubChem enrichment dataset",
    ),
    "assay": PipelineCommandConfig(
        pipeline_name="assay",
        pipeline_factory=lambda: AssayPipeline,
        default_config=get_config_path("pipelines/assay.yaml"),
        default_input=Path("data/input/assay.csv"),
        default_output_dir=Path("data/output/assay"),
        description="ChEMBL assay data",
    ),
    "target": PipelineCommandConfig(
        pipeline_name="target",
        pipeline_factory=lambda: TargetPipeline,
        default_config=get_config_path("pipelines/target.yaml"),
        default_input=Path("data/input/target.csv"),
        default_output_dir=Path("data/output/target"),
        mode_choices=("default", "smoke"),
        description="ChEMBL + UniProt + IUPHAR",
    ),
    "document": PipelineCommandConfig(
        pipeline_name="document",
        pipeline_factory=lambda: DocumentPipeline,
        default_config=get_config_path("pipelines/document.yaml"),
        default_input=Path("data/input/document.csv"),
        default_output_dir=Path("data/output/documents"),
        mode_choices=DOCUMENT_PIPELINE_MODES,
        default_mode=DEFAULT_DOCUMENT_PIPELINE_MODE,
        description="ChEMBL + external sources",
    ),
    "testitem": PipelineCommandConfig(
        pipeline_name="testitem",
        pipeline_factory=lambda: TestItemPipeline,
        default_config=get_config_path("pipelines/testitem.yaml"),
        default_input=Path("data/input/testitem.csv"),
        default_output_dir=Path("data/output/testitems"),
        description="ChEMBL molecules + PubChem",
    ),
    "gtp_iuphar": PipelineCommandConfig(
        pipeline_name="gtp_iuphar",
        pipeline_factory=lambda: GtpIupharPipeline,
        default_config=get_config_path("pipelines/iuphar.yaml"),
        default_input=Path("data/input/iuphar_targets.csv"),
        default_output_dir=Path("data/output/iuphar"),
        description="Guide to Pharmacology targets",
    ),
    "uniprot": PipelineCommandConfig(
        pipeline_name="uniprot",
        pipeline_factory=lambda: UniProtPipeline,
        default_config=get_config_path("pipelines/uniprot.yaml"),
        default_input=Path("data/input/uniprot.csv"),
        default_output_dir=Path("data/output/uniprot"),
        description="Standalone UniProt enrichment",
    ),
}


def get_pipeline_command_config(key: str) -> PipelineCommandConfig:
    try:
        config = PIPELINE_COMMAND_REGISTRY[key]
    except KeyError as exc:  # pragma: no cover - defensive branch
        raise KeyError(f"Unknown pipeline registry key: {key}") from exc
    return replace(config)


def register_pipeline_command(app: typer.Typer, key: str) -> None:
    command = create_pipeline_command(get_pipeline_command_config(key))
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
