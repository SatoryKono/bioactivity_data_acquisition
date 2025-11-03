from __future__ import annotations

import importlib
import importlib.util
import sys
from pathlib import Path

import pytest

PACKAGE_MODULES = [
    "bioetl.sources.pubmed.client",
    "bioetl.sources.openalex.client",
    "bioetl.sources.iuphar.client",
    "bioetl.sources.crossref.client",
    "bioetl.sources.chembl.activity.client",
    "bioetl.sources.chembl.assay.client",
    "bioetl.sources.chembl.target.client",
    "bioetl.sources.chembl.testitem.client",
    "bioetl.sources.chembl.document.client",
    "bioetl.sources.pubchem.client",
    "bioetl.sources.semantic_scholar.client",
    "bioetl.sources.uniprot.client",
]

MODULE_IMPORTS = [
    "bioetl.sources.chembl.activity.client.activity_client",
    "bioetl.sources.chembl.assay.client.assay_client",
    "bioetl.sources.chembl.target.client.chembl_client",
    "bioetl.sources.chembl.testitem.client.client",
    "bioetl.sources.chembl.document.client.document_client",
    "bioetl.sources.pubchem.client.pubchem_client",
    "bioetl.sources.uniprot.client.search_client",
    "bioetl.sources.uniprot.client.orthologs_client",
    "bioetl.sources.uniprot.client.idmapping_client",
]

FILE_BASE = Path(__file__).resolve().parents[3]
FILE_MODULES = {
    "tests.legacy_clients.uniprot_module": FILE_BASE / "src/bioetl/sources/uniprot/client.py",
}


@pytest.mark.parametrize("module_name", PACKAGE_MODULES)
def test_legacy_client_packages_emit_warning(module_name: str) -> None:
    """Importing legacy client packages should raise DeprecationWarning."""

    sys.modules.pop(module_name, None)
    with pytest.warns(DeprecationWarning):
        importlib.import_module(module_name)


@pytest.mark.parametrize("module_name", MODULE_IMPORTS)
def test_legacy_client_modules_emit_warning(module_name: str) -> None:
    """Importing legacy client implementations should raise DeprecationWarning."""

    sys.modules.pop(module_name, None)
    with pytest.warns(DeprecationWarning):
        importlib.import_module(module_name)


@pytest.mark.parametrize("module_name", list(FILE_MODULES))
def test_legacy_client_files_emit_warning(module_name: str) -> None:
    """Directly loading legacy modules from disk should raise DeprecationWarning."""

    path = FILE_MODULES[module_name]
    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(module_name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        with pytest.warns(DeprecationWarning):
            spec.loader.exec_module(module)
    finally:
        sys.modules.pop(module_name, None)
