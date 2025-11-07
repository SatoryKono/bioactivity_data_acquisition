"""Document entity clients for ChEMBL API."""

from __future__ import annotations

from typing import TYPE_CHECKING

from bioetl.clients.chembl_base import ChemblEntityFetcher, EntityConfig

if TYPE_CHECKING:
    from bioetl.clients import ChemblClient

__all__ = ["ChemblDocumentTermEntityClient"]


class ChemblDocumentTermEntityClient(ChemblEntityFetcher):
    """Клиент для получения document_term записей из ChEMBL API."""

    def __init__(self, chembl_client: ChemblClient) -> None:
        """Инициализировать клиент для document_term.

        Parameters
        ----------
        chembl_client:
            Экземпляр ChemblClient для выполнения запросов.
        """
        config = EntityConfig(
            endpoint="/document_term.json",
            filter_param="document_chembl_id__in",
            id_key="document_chembl_id",
            items_key="document_terms",
            log_prefix="document_term",
            chunk_size=100,
            supports_list_result=True,  # Один документ может иметь несколько терминов
        )
        super().__init__(chembl_client, config)
