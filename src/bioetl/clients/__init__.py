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
from .document.chembl_document import ChemblDocumentClient
from .document.chembl_document_entity import ChemblDocumentTermEntityClient
from .target.chembl_target import ChemblTargetClient
from .testitem.chembl_testitem import ChemblTestitemClient

# Импорт ChemblClient из модуля chembl.py (не из пакета chembl/)
# Используем importlib для импорта из файла напрямую, чтобы избежать циклических зависимостей
# Это должно быть после обычных импортов, поэтому используем noqa для подавления предупреждения
_chembl_module_path = Path(__file__).parent / "chembl.py"  # noqa: E402
_spec = importlib.util.spec_from_file_location("bioetl.clients.chembl_client", _chembl_module_path)  # noqa: E402
if _spec is None or _spec.loader is None:  # noqa: E402
    msg = f"Failed to load module from {_chembl_module_path}"
    raise ImportError(msg)
_chembl_module = importlib.util.module_from_spec(_spec)  # noqa: E402
_spec.loader.exec_module(_chembl_module)  # noqa: E402
ChemblClient = _chembl_module.ChemblClient  # noqa: E402

__all__ = [
    "ChemblClient",
    "ChemblAssayClient",
    "ChemblActivityClient",
    "ChemblDocumentClient",
    "ChemblTargetClient",
    "ChemblTestitemClient",
    # Новые специализированные клиенты (для расширенного использования)
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
