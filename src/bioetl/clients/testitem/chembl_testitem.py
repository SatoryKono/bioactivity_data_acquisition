"""Testitem-specific HTTP client helpers built on top of :mod:`bioetl.clients.chembl`."""

from __future__ import annotations

from typing import ClassVar

from bioetl.clients.chembl_base import ChemblClientProtocol
from bioetl.clients.chembl_entity_client import ChemblEntityClientBase

__all__ = ["ChemblTestitemClient"]


class ChemblTestitemClient(ChemblEntityClientBase):
    """High level helper focused on retrieving molecule (testitem) payloads."""

    ENTITY_KEY: ClassVar[str] = "testitem"

    def __init__(
        self,
        chembl_client: ChemblClientProtocol,
        *,
        batch_size: int = 25,
        max_url_length: int | None = None,
    ) -> None:
        """Инициализировать клиент для molecule (testitem).

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
