"""HTTP clients for specific upstream APIs."""

import importlib.util
from pathlib import Path

from .activity.chembl_activity import ChemblActivityClient
from .assay.chembl_assay import ChemblAssayClient
from .assay.chembl_assay_entity import ChemblAssayEntityClient
from .chembl_base import ChemblEntityFetcher, EntityConfig
from .chembl_entities import (
    ChemblAssayClassificationEntityClient,
    ChemblAssayClassMapEntityClient,
    ChemblAssayParametersEntityClient,
    ChemblCompoundRecordEntityClient,
    ChemblDataValidityEntityClient,
    ChemblMoleculeEntityClient,
)
from .chembl_iterator import ChemblEntityIterator
from .types import EntityClient
from .document.chembl_document import ChemblDocumentClient
from .document.chembl_document_entity import ChemblDocumentTermEntityClient
from .target.chembl_target import ChemblTargetClient
from .testitem.chembl_testitem import ChemblTestitemClient

# Импорт ChemblClient из модуля chembl.py (не из пакета chembl/)
# Используем importlib для импорта из файла напрямую, чтобы избежать циклических зависимостей
# Это должно быть после обычных импортов, поэтому используем noqa для подавления предупреждения
_CHEMBL_MODULE_PATH = Path(__file__).parent / "chembl.py"  # noqa: E402
_CHEMBL_SPEC = importlib.util.spec_from_file_location("bioetl.clients.chembl_client", _CHEMBL_MODULE_PATH)  # noqa: E402
if _CHEMBL_SPEC is None or _CHEMBL_SPEC.loader is None:  # noqa: E402
    msg = f"Failed to load module from {_CHEMBL_MODULE_PATH}"
    raise ImportError(msg)
_CHEMBL_MODULE = importlib.util.module_from_spec(_CHEMBL_SPEC)  # noqa: E402
_CHEMBL_SPEC.loader.exec_module(_CHEMBL_MODULE)  # noqa: E402
ChemblClient = _CHEMBL_MODULE.ChemblClient  # noqa: E402

__all__ = [
    "ChemblClient",
    "ChemblAssayClient",
    "ChemblActivityClient",
    "ChemblDocumentClient",
    "ChemblTargetClient",
    "ChemblTestitemClient",
    # Новые специализированные клиенты (для расширенного использования)
    "EntityClient",
    "ChemblEntityFetcher",
    "ChemblEntityIterator",
    "EntityConfig",
    "ChemblAssayEntityClient",
    "ChemblMoleculeEntityClient",
    "ChemblDataValidityEntityClient",
    "ChemblDocumentTermEntityClient",
    "ChemblAssayClassMapEntityClient",
    "ChemblAssayParametersEntityClient",
    "ChemblAssayClassificationEntityClient",
    "ChemblCompoundRecordEntityClient",
]
