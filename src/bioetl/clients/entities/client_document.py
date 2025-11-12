"""Document-specific HTTP client helpers built on top of :mod:`bioetl.clients.client_chembl`."""

from __future__ import annotations

from typing import Any

from bioetl.clients.client_chembl_base import EntityConfig
from bioetl.clients.client_chembl_iterator import ChemblEntityIteratorBase

__all__ = ["ChemblDocumentClient"]


class ChemblDocumentClient(ChemblEntityIteratorBase):
    """High level helper focused on retrieving document payloads."""

    def __init__(
        self,
        chembl_client: Any,  # ChemblClient
        *,
        batch_size: int = 25,
        max_url_length: int | None = None,
    ) -> None:
        """Инициализировать клиент для document.

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
            endpoint="/document.json",
            filter_param="document_chembl_id__in",
            id_key="document_chembl_id",
            items_key="documents",
            log_prefix="document",
            chunk_size=100,
            supports_list_result=False,
            base_endpoint_length=len("/document.json?"),
            enable_url_length_check=False,
        )

        super().__init__(
            chembl_client=chembl_client,
            config=config,
            batch_size=batch_size,
            max_url_length=max_url_length,
        )

