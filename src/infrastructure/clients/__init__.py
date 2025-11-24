"""HTTP clients for specific upstream APIs.

The public surface of the ``infrastructure.clients`` package is currently being refactored.
To avoid importing partially migrated modules during CLI bootstrap, this package
deliberately refrains from loading any submodules on import.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any, Final

if TYPE_CHECKING:
    from infrastructure.clients.chembl_config import EntityConfig
    from infrastructure.clients.chembl_entity_factory import (
        ChemblClientBundle,
        ChemblEntityClientFactory,
    )
    from infrastructure.clients.chembl_entity_registry import (
        ChemblEntityDefinition,
        ChemblEntityRegistryError,
        get_entity_definition,
        iter_entity_definitions,
        register_entity_definition,
    )
    from infrastructure.clients.client_chembl import ChemblClient
    from infrastructure.clients.client_chembl_entity_base import (
        ChemblEntityFetcherBase,
    )
    from infrastructure.clients.entities.client_activity import ChemblActivityClient
    from infrastructure.clients.entities.client_assay import ChemblAssayClient
    from infrastructure.clients.entities.client_assay_class_map import (
        ChemblAssayClassMapEntityClient,
    )
    from infrastructure.clients.entities.client_assay_classification import (
        ChemblAssayClassificationEntityClient,
    )
    from infrastructure.clients.entities.client_assay_entity import (
        ChemblAssayEntityClient,
    )
    from infrastructure.clients.entities.client_assay_parameters import (
        ChemblAssayParametersEntityClient,
    )
    from infrastructure.clients.entities.client_compound_record import (
        ChemblCompoundRecordEntityClient,
    )
    from infrastructure.clients.entities.client_data_validity import (
        ChemblDataValidityEntityClient,
    )
    from infrastructure.clients.entities.client_document import ChemblDocumentClient
    from infrastructure.clients.entities.client_document_term import (
        ChemblDocumentTermEntityClient,
    )
    from infrastructure.clients.entities.client_molecule import (
        ChemblMoleculeEntityClient,
    )
    from infrastructure.clients.entities.client_target import ChemblTargetClient
    from infrastructure.clients.entities.client_testitem import ChemblTestitemClient

__all__ = [
    "ChemblClient",
    "ChemblAssayClient",
    "ChemblAssayEntityClient",
    "ChemblActivityClient",
    "ChemblDocumentClient",
    "ChemblTargetClient",
    "ChemblTestitemClient",
    "ChemblEntityFetcherBase",
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
    "ChemblClient": ("infrastructure.clients.client_chembl", "ChemblClient"),
    "ChemblAssayClient": (
        "infrastructure.clients.entities.client_assay",
        "ChemblAssayClient",
    ),
    "ChemblAssayEntityClient": (
        "infrastructure.clients.entities.client_assay_entity",
        "ChemblAssayEntityClient",
    ),
    "ChemblEntityFetcherBase": (
        "infrastructure.clients.client_chembl_entity_base",
        "ChemblEntityFetcherBase",
    ),
    "EntityConfig": ("infrastructure.clients.chembl_config", "EntityConfig"),
    "ChemblActivityClient": (
        "infrastructure.clients.entities.client_activity",
        "ChemblActivityClient",
    ),
    "ChemblDocumentClient": (
        "infrastructure.clients.entities.client_document",
        "ChemblDocumentClient",
    ),
    "ChemblTargetClient": (
        "infrastructure.clients.entities.client_target",
        "ChemblTargetClient",
    ),
    "ChemblTestitemClient": (
        "infrastructure.clients.entities.client_testitem",
        "ChemblTestitemClient",
    ),
    "ChemblMoleculeEntityClient": (
        "infrastructure.clients.entities.client_molecule",
        "ChemblMoleculeEntityClient",
    ),
    "ChemblDataValidityEntityClient": (
        "infrastructure.clients.entities.client_data_validity",
        "ChemblDataValidityEntityClient",
    ),
    "ChemblDocumentTermEntityClient": (
        "infrastructure.clients.entities.client_document_term",
        "ChemblDocumentTermEntityClient",
    ),
    "ChemblAssayClassMapEntityClient": (
        "infrastructure.clients.entities.client_assay_class_map",
        "ChemblAssayClassMapEntityClient",
    ),
    "ChemblAssayParametersEntityClient": (
        "infrastructure.clients.entities.client_assay_parameters",
        "ChemblAssayParametersEntityClient",
    ),
    "ChemblAssayClassificationEntityClient": (
        "infrastructure.clients.entities.client_assay_classification",
        "ChemblAssayClassificationEntityClient",
    ),
    "ChemblCompoundRecordEntityClient": (
        "infrastructure.clients.entities.client_compound_record",
        "ChemblCompoundRecordEntityClient",
    ),
    "ChemblEntityClientFactory": (
        "infrastructure.clients.chembl_entity_factory",
        "ChemblEntityClientFactory",
    ),
    "ChemblClientBundle": (
        "infrastructure.clients.chembl_entity_factory",
        "ChemblClientBundle",
    ),
    "ChemblEntityDefinition": (
        "infrastructure.clients.chembl_entity_registry",
        "ChemblEntityDefinition",
    ),
    "ChemblEntityRegistryError": (
        "infrastructure.clients.chembl_entity_registry",
        "ChemblEntityRegistryError",
    ),
    "get_entity_definition": (
        "infrastructure.clients.chembl_entity_registry",
        "get_entity_definition",
    ),
    "iter_entity_definitions": (
        "infrastructure.clients.chembl_entity_registry",
        "iter_entity_definitions",
    ),
    "register_entity_definition": (
        "infrastructure.clients.chembl_entity_registry",
        "register_entity_definition",
    ),
}


def __getattr__(name: str) -> Any:
    """Lazily resolve client symbols to avoid import-time side effects."""
    try:
        module_path, attr_name = _ATTR_MAP[name]
    except KeyError as exc:
        raise AttributeError(
            f"module {__name__!r} has no attribute {name!r}"
        ) from exc

    module = import_module(module_path)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value
