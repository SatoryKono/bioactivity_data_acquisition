"""Smoke tests for script registry imports."""

from __future__ import annotations

import importlib

import pytest

from scripts import PIPELINE_REGISTRY


@pytest.mark.parametrize(
    ("key", "module_path", "attr_name"),
    [
        ("chembl_activity", "bioetl.pipelines.chembl_activity", "ActivityPipeline"),
        ("chembl_assay", "bioetl.pipelines.chembl_assay", "AssayPipeline"),
        ("chembl_target", "bioetl.pipelines.chembl_target", "TargetPipeline"),
        ("chembl_document", "bioetl.pipelines.chembl_document", "DocumentPipeline"),
        ("chembl_testitem", "bioetl.pipelines.chembl_testitem", "TestItemPipeline"),
        ("pubchem_molecule", "bioetl.sources.pubchem.pipeline", "PubChemPipeline"),
        ("uniprot_protein", "bioetl.sources.uniprot.pipeline", "UniProtPipeline"),
        ("gtp_iuphar", "bioetl.sources.iuphar.pipeline", "GtpIupharPipeline"),
    ],
)
def test_cli_registry_resolves_pipeline_imports(key: str, module_path: str, attr_name: str) -> None:
    module = importlib.import_module(module_path)
    pipeline_cls = getattr(module, attr_name)

    config = PIPELINE_REGISTRY[key]

    assert config.pipeline_factory() is pipeline_cls
    assert getattr(module, attr_name) is pipeline_cls
