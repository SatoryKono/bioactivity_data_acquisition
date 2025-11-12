"""Document entity clients for ChEMBL API."""

from __future__ import annotations

from bioetl.clients.base_entity import BaseEntityClient
from bioetl.clients.chembl_base import EntityConfig

__all__ = ["ChemblDocumentTermEntityClient"]


class ChemblDocumentTermEntityClient(BaseEntityClient):
    """Клиент для получения document_term записей из ChEMBL API."""

    CONFIG = EntityConfig(
        endpoint="/document_term.json",
        filter_param="document_chembl_id__in",
        id_key="document_chembl_id",
        items_key="document_terms",
        log_prefix="document_term",
        chunk_size=100,
        supports_list_result=True,  # Один документ может иметь несколько терминов
    )
