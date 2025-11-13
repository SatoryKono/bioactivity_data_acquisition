"""Document entity clients for ChEMBL API."""

from __future__ import annotations

from typing import ClassVar

from bioetl.clients.client_chembl_base import EntityConfig, make_entity_config
from bioetl.clients.client_chembl_entity import ChemblEntityClientBase

__all__ = ["ChemblDocumentTermEntityClient"]


class ChemblDocumentTermEntityClient(ChemblEntityClientBase):
    """Client for retrieving ``document_term`` records from the ChEMBL API."""

    CONFIG: ClassVar[EntityConfig] = make_entity_config(
        endpoint="/document_term.json",
        filter_param="document_chembl_id__in",
        id_key="document_chembl_id",
        items_key="document_terms",
        log_prefix="document_term",
        chunk_size=100,
        supports_list_result=True,  # A single document may expose multiple terms
    )

