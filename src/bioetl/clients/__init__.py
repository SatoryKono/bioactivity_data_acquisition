"""HTTP clients for specific upstream APIs."""
from __future__ import annotations

from bioetl.clients.chembl_config import EntityConfig
from bioetl.clients.chembl_entity_factory import (
    ChemblClientBundle,
    ChemblEntityClientFactory,
)
from bioetl.clients.chembl_entity_registry import (
    ChemblEntityDefinition,
    ChemblEntityRegistryError,
    get_entity_definition,
    iter_entity_definitions,
    register_entity_definition,
)
from bioetl.clients.client_chembl import ChemblClient
from bioetl.clients.client_chembl_entity_base import ChemblEntityFetcherBase
from bioetl.clients.entities.client_activity import ChemblActivityClient
from bioetl.clients.entities.client_assay import ChemblAssayClient
from bioetl.clients.entities.client_assay_class_map import ChemblAssayClassMapEntityClient
from bioetl.clients.entities.client_assay_classification import (
    ChemblAssayClassificationEntityClient,
)
from bioetl.clients.entities.client_assay_entity import ChemblAssayEntityClient
from bioetl.clients.entities.client_assay_parameters import ChemblAssayParametersEntityClient
from bioetl.clients.entities.client_compound_record import (
    ChemblCompoundRecordEntityClient,
)
from bioetl.clients.entities.client_data_validity import ChemblDataValidityEntityClient
from bioetl.clients.entities.client_document import ChemblDocumentClient
from bioetl.clients.entities.client_document_term import ChemblDocumentTermEntityClient
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
