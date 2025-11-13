"""HTTP clients for specific upstream APIs.

The public surface of the ``bioetl.clients`` package is currently being refactored.
To avoid importing partially migrated modules during CLI bootstrap, this package
deliberately refrains from loading any submodules on import.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any, Final

if TYPE_CHECKING:
    from bioetl.clients.chembl_config import EntityConfig
    from bioetl.clients.client_chembl_base import ChemblEntityFetcherBase
    from bioetl.clients.client_chembl_common import ChemblClient
    from bioetl.clients.client_chembl_entity import ChemblEntityClientBase
    from bioetl.clients.client_chembl_iterator import (
        ChemblEntityIterator,
        ChemblEntityIteratorBase,
    )
    from bioetl.clients.entities.client_activity import ChemblActivityClient
    from bioetl.clients.entities.client_assay import ChemblAssayClient
    from bioetl.clients.entities.client_assay_class_map import (
        ChemblAssayClassMapEntityClient,
    )
    from bioetl.clients.entities.client_assay_classification import (
        ChemblAssayClassificationEntityClient,
    )
    from bioetl.clients.entities.client_assay_entity import ChemblAssayEntityClient
    from bioetl.clients.entities.client_assay_parameters import (
        ChemblAssayParametersEntityClient,
    )
    from bioetl.clients.entities.client_compound_record import (
        ChemblCompoundRecordEntityClient,
    )
    from bioetl.clients.entities.client_data_validity import (
        ChemblDataValidityEntityClient,
    )
    from bioetl.clients.entities.client_document import ChemblDocumentClient
    from bioetl.clients.entities.client_document_term import (
        ChemblDocumentTermEntityClient,
    )
    from bioetl.clients.entities.client_molecule import ChemblMoleculeEntityClient
    from bioetl.clients.entities.client_target import ChemblTargetClient
    from bioetl.clients.entities.client_testitem import ChemblTestitemClient

__all__ = [
    "ChemblClient",
    "ChemblAssayClient",
    "ChemblAssayEntityClient",
    "ChemblActivityClient",
    "ChemblDocumentClient",
    "ChemblTargetClient",
    "ChemblTestitemClient",
    "ChemblEntityFetcherBase",
    "ChemblEntityIteratorBase",
    "ChemblEntityIterator",
    "ChemblEntityClientBase",
    "EntityConfig",
    "ChemblMoleculeEntityClient",
    "ChemblDataValidityEntityClient",
    "ChemblDocumentTermEntityClient",
    "ChemblAssayClassMapEntityClient",
    "ChemblAssayParametersEntityClient",
    "ChemblAssayClassificationEntityClient",
    "ChemblCompoundRecordEntityClient",
    "ChemblEntityClientFactory",
    "ChemblClientBundle",
    "ChemblEntityDefinition",
    "ChemblEntityRegistryError",
    "get_entity_definition",
    "iter_entity_definitions",
    "register_entity_definition",
]

_ATTR_MAP: Final[dict[str, tuple[str, str]]] = {
    "ChemblClient": ("bioetl.clients.client_chembl_common", "ChemblClient"),
    "ChemblAssayClient": ("bioetl.clients.entities.client_assay", "ChemblAssayClient"),
    "ChemblAssayEntityClient": (
        "bioetl.clients.entities.client_assay_entity",
        "ChemblAssayEntityClient",
    ),
    "ChemblEntityFetcherBase": (
        "bioetl.clients.client_chembl_base",
        "ChemblEntityFetcherBase",
    ),
    "ChemblEntityIteratorBase": (
        "bioetl.clients.client_chembl_iterator",
        "ChemblEntityIteratorBase",
    ),
    "ChemblEntityIterator": ("bioetl.clients.client_chembl_iterator", "ChemblEntityIterator"),
    "ChemblEntityClientBase": (
        "bioetl.clients.client_chembl_entity",
        "ChemblEntityClientBase",
    ),
    "EntityConfig": ("bioetl.clients.chembl_config", "EntityConfig"),
    "ChemblActivityClient": ("bioetl.clients.entities.client_activity", "ChemblActivityClient"),
    "ChemblDocumentClient": ("bioetl.clients.entities.client_document", "ChemblDocumentClient"),
    "ChemblTargetClient": ("bioetl.clients.entities.client_target", "ChemblTargetClient"),
    "ChemblTestitemClient": ("bioetl.clients.entities.client_testitem", "ChemblTestitemClient"),
    "ChemblMoleculeEntityClient": (
        "bioetl.clients.entities.client_molecule",
        "ChemblMoleculeEntityClient",
    ),
    "ChemblDataValidityEntityClient": (
        "bioetl.clients.entities.client_data_validity",
        "ChemblDataValidityEntityClient",
    ),
    "ChemblDocumentTermEntityClient": (
        "bioetl.clients.entities.client_document_term",
        "ChemblDocumentTermEntityClient",
    ),
    "ChemblAssayClassMapEntityClient": (
        "bioetl.clients.entities.client_assay_class_map",
        "ChemblAssayClassMapEntityClient",
    ),
    "ChemblAssayParametersEntityClient": (
        "bioetl.clients.entities.client_assay_parameters",
        "ChemblAssayParametersEntityClient",
    ),
    "ChemblAssayClassificationEntityClient": (
        "bioetl.clients.entities.client_assay_classification",
        "ChemblAssayClassificationEntityClient",
    ),
    "ChemblCompoundRecordEntityClient": (
        "bioetl.clients.entities.client_compound_record",
        "ChemblCompoundRecordEntityClient",
    ),
    "ChemblEntityClientFactory": (
        "bioetl.clients.chembl_entity_factory",
        "ChemblEntityClientFactory",
    ),
    "ChemblClientBundle": (
        "bioetl.clients.chembl_entity_factory",
        "ChemblClientBundle",
    ),
    "ChemblEntityDefinition": (
        "bioetl.clients.chembl_entity_registry",
        "ChemblEntityDefinition",
    ),
    "ChemblEntityRegistryError": (
        "bioetl.clients.chembl_entity_registry",
        "ChemblEntityRegistryError",
    ),
    "get_entity_definition": (
        "bioetl.clients.chembl_entity_registry",
        "get_entity_definition",
    ),
    "iter_entity_definitions": (
        "bioetl.clients.chembl_entity_registry",
        "iter_entity_definitions",
    ),
    "register_entity_definition": (
        "bioetl.clients.chembl_entity_registry",
        "register_entity_definition",
    ),
}


def __getattr__(name: str) -> Any:
    """Lazily resolve client symbols to avoid import-time side effects."""
    try:
        module_path, attr_name = _ATTR_MAP[name]
    except KeyError as exc:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from exc

    module = import_module(module_path)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value
