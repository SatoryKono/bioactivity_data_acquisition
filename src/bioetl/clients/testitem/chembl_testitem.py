"""Testitem-specific HTTP client helpers built on top of :mod:`bioetl.clients.chembl`."""

from __future__ import annotations

from typing import Any

from bioetl.clients.chembl_base import EntityConfig
from bioetl.clients.chembl_iterator import ChemblEntityIterator

__all__ = ["ChemblTestitemClient"]


class ChemblTestitemClient(ChemblEntityIterator):
    """High level helper focused on retrieving molecule (testitem) payloads."""

    def __init__(
        self,
        chembl_client: Any,  # ChemblClient
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
        config = EntityConfig(
            endpoint="/molecule.json",
            filter_param="molecule_chembl_id__in",
            id_key="molecule_chembl_id",
            items_key="molecules",
            log_prefix="molecule",
            chunk_size=100,
            supports_list_result=False,
            base_endpoint_length=len("/molecule.json?"),
            enable_url_length_check=False,
        )

        super().__init__(
            chembl_client=chembl_client,
            config=config,
            batch_size=batch_size,
            max_url_length=max_url_length,
        )
