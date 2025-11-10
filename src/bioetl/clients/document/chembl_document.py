"""Document-specific HTTP client helpers built on top of :mod:`bioetl.clients.chembl`."""

from __future__ import annotations

from bioetl.clients.chembl_base import ChemblClientProtocol, EntityConfig
from bioetl.clients.chembl_entity_client import ChemblEntityClientBase

__all__ = ["ChemblDocumentClient"]


class ChemblDocumentClient(ChemblEntityClientBase):
    """High level helper focused on retrieving document payloads."""

    def __init__(
        self,
        chembl_client: ChemblClientProtocol,
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
        super().__init__(
            chembl_client=chembl_client,
            batch_size=batch_size,
            max_url_length=max_url_length,
        )

    @classmethod
    def _create_config(cls, max_url_length: int | None) -> EntityConfig:
        return EntityConfig(
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
