"""Smoke tests for ChEMBL entity client import paths."""

from __future__ import annotations

import importlib
from types import ModuleType

import pytest  # type: ignore[reportMissingImports]

ENTITY_IMPORTS: tuple[tuple[str, str], ...] = (
    ("bioetl.clients.entities.client_activity", "ChemblActivityClient"),
    ("bioetl.clients.entities.client_testitem", "ChemblTestitemClient"),
    ("bioetl.clients.entities.client_target", "ChemblTargetClient"),
    ("bioetl.clients.entities.client_document", "ChemblDocumentClient"),
    ("bioetl.clients.entities.client_document_term", "ChemblDocumentTermEntityClient"),
    ("bioetl.clients.entities.client_molecule", "ChemblMoleculeEntityClient"),
    ("bioetl.clients.entities.client_data_validity", "ChemblDataValidityEntityClient"),
    ("bioetl.clients.entities.client_assay_class_map", "ChemblAssayClassMapEntityClient"),
    (
        "bioetl.clients.entities.client_assay_parameters",
        "ChemblAssayParametersEntityClient",
    ),
    (
        "bioetl.clients.entities.client_assay_classification",
        "ChemblAssayClassificationEntityClient",
    ),
    ("bioetl.clients.entities.client_compound_record", "ChemblCompoundRecordEntityClient"),
)


@pytest.mark.smoke  # type: ignore[reportUnknownMemberType]
@pytest.mark.unit  # type: ignore[reportUnknownMemberType]
@pytest.mark.parametrize(  # type: ignore[reportUnknownMemberType]
    "module_name,symbol",
    ENTITY_IMPORTS,
)
def test_entity_import_smoke(module_name: str, symbol: str) -> None:
    """Ensure that ChEMBL entity client modules expose expected symbols."""
    module: ModuleType = importlib.import_module(module_name)
    exported = getattr(module, symbol, None)

    assert exported is not None, f"{symbol} missing in {module_name}"
    assert getattr(exported, "__module__", module_name) == module_name

