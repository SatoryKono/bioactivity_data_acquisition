"""Assay-specific HTTP client helpers built on top of :mod:`bioetl.clients.chembl`."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import ClassVar

from bioetl.clients.assay.chembl_assay_entity import ChemblAssayEntityClient
from bioetl.clients.chembl_base import ChemblClientProtocol
from bioetl.clients.chembl_entity_client import ChemblEntityClientBase

# Import ChemblClient directly from the module to avoid conflict with chembl/ package
# Use importlib to load from chembl.py file, not from chembl/ package
# Path updated: now in assay/ subdirectory, so chembl.py is in parent directory
_CHEMBL_MODULE_PATH = Path(__file__).parent.parent / "chembl.py"
_CHEMBL_SPEC = importlib.util.spec_from_file_location(
    "bioetl.clients.chembl_client", _CHEMBL_MODULE_PATH
)
if _CHEMBL_SPEC is not None and _CHEMBL_SPEC.loader is not None:
    _CHEMBL_MODULE = importlib.util.module_from_spec(_CHEMBL_SPEC)
    _CHEMBL_SPEC.loader.exec_module(_CHEMBL_MODULE)
    ChemblClient = _CHEMBL_MODULE.ChemblClient
else:
    raise ImportError("Failed to load chembl.py module")


class ChemblAssayClient(ChemblEntityClientBase):
    """High level helper focused on retrieving assay payloads."""

    ENTITY_KEY: ClassVar[str] = "assay"

    def __init__(
        self,
        chembl_client: ChemblClientProtocol,
        *,
        batch_size: int = 25,
        max_url_length: int | None = None,
    ) -> None:
        """Инициализировать клиент для assay.

        Parameters
        ----------
        chembl_client:
            Экземпляр ChemblClient для выполнения запросов.
        batch_size:
            Размер батча для пагинации (максимум 25 для ChEMBL API).
        max_url_length:
            Максимальная длина URL для проверки. Если None, проверка отключена.
        """
        super().__init__(
            chembl_client=chembl_client,
            batch_size=batch_size,
            max_url_length=max_url_length,
        )

        # Используем унифицированный entity client для получения по ID
        self._entity_client = ChemblAssayEntityClient(chembl_client)

