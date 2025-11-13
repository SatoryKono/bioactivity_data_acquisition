"""Entity-specific clients for ChEMBL API (modularized package)."""

from .client_activity import ChemblActivityClient
from .client_assay import ChemblAssayClient
from .client_assay_class_map import ChemblAssayClassMapEntityClient
from .client_assay_classification import ChemblAssayClassificationEntityClient
from .client_assay_entity import ChemblAssayEntityClient
from .client_assay_parameters import ChemblAssayParametersEntityClient
from .client_compound_record import ChemblCompoundRecordEntityClient
from .client_data_validity import ChemblDataValidityEntityClient
from .client_document import ChemblDocumentClient
from .client_document_term import ChemblDocumentTermEntityClient
from .client_molecule import ChemblMoleculeEntityClient
from .client_target import ChemblTargetClient
from .client_testitem import ChemblTestitemClient

__all__: list[str] = [
    "ChemblActivityClient",
    "ChemblAssayClient",
    "ChemblAssayEntityClient",
    "ChemblTestitemClient",
    "ChemblTargetClient",
    "ChemblDocumentClient",
    "ChemblDocumentTermEntityClient",
    "ChemblMoleculeEntityClient",
    "ChemblDataValidityEntityClient",
    "ChemblAssayClassMapEntityClient",
    "ChemblAssayParametersEntityClient",
    "ChemblAssayClassificationEntityClient",
    "ChemblCompoundRecordEntityClient",
]

