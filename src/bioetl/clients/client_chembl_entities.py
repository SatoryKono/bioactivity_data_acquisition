"""Deprecated shim for relocated ChEMBL entity clients."""

from __future__ import annotations

from bioetl.clients.entities.client_assay_classification import (
    ChemblAssayClassificationEntityClient as _ChemblAssayClassificationEntityClient,
)
from bioetl.clients.entities.client_assay_class_map import (
    ChemblAssayClassMapEntityClient as _ChemblAssayClassMapEntityClient,
)
from bioetl.clients.entities.client_assay_parameters import (
    ChemblAssayParametersEntityClient as _ChemblAssayParametersEntityClient,
)
from bioetl.clients.entities.client_compound_record import (
    ChemblCompoundRecordEntityClient as _ChemblCompoundRecordEntityClient,
)
from bioetl.clients.entities.client_data_validity import (
    ChemblDataValidityEntityClient as _ChemblDataValidityEntityClient,
)
from bioetl.clients.entities.client_molecule import (
    ChemblMoleculeEntityClient as _ChemblMoleculeEntityClient,
)

ChemblMoleculeEntityClient = _ChemblMoleculeEntityClient
ChemblMoleculeEntityClient.__module__ = __name__
ChemblDataValidityEntityClient = _ChemblDataValidityEntityClient
ChemblDataValidityEntityClient.__module__ = __name__
ChemblAssayClassMapEntityClient = _ChemblAssayClassMapEntityClient
ChemblAssayClassMapEntityClient.__module__ = __name__
ChemblAssayParametersEntityClient = _ChemblAssayParametersEntityClient
ChemblAssayParametersEntityClient.__module__ = __name__
ChemblAssayClassificationEntityClient = _ChemblAssayClassificationEntityClient
ChemblAssayClassificationEntityClient.__module__ = __name__
ChemblCompoundRecordEntityClient = _ChemblCompoundRecordEntityClient
ChemblCompoundRecordEntityClient.__module__ = __name__

__all__ = [
    "ChemblMoleculeEntityClient",
    "ChemblDataValidityEntityClient",
    "ChemblAssayClassMapEntityClient",
    "ChemblAssayParametersEntityClient",
    "ChemblAssayClassificationEntityClient",
    "ChemblCompoundRecordEntityClient",
]
