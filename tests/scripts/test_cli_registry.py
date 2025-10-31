"""Smoke tests for script registry imports."""

from __future__ import annotations

import importlib

import pytest

from scripts import PIPELINE_COMMAND_REGISTRY


@pytest.mark.parametrize(
    ("key", "module_path", "attr_name"),
    [
        ("activity", "bioetl.sources.chembl.activity.pipeline", "ActivityPipeline"),
        ("assay", "bioetl.sources.chembl.assay.pipeline", "AssayPipeline"),
        ("target", "bioetl.sources.chembl.target.pipeline", "TargetPipeline"),
        ("document", "bioetl.sources.chembl.document.pipeline", "DocumentPipeline"),
        ("testitem", "bioetl.sources.chembl.testitem.pipeline", "TestItemPipeline"),
        ("pubchem", "bioetl.sources.pubchem.pipeline", "PubChemPipeline"),
    ],
)
def test_cli_registry_resolves_pipeline_imports(key: str, module_path: str, attr_name: str) -> None:
    module = importlib.import_module(module_path)
    pipeline_cls = getattr(module, attr_name)

    config = PIPELINE_COMMAND_REGISTRY[key]

    assert config.pipeline_factory() is pipeline_cls
    assert getattr(module, attr_name) is pipeline_cls
