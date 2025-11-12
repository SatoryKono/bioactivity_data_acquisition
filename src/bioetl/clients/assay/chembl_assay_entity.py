"""Assay entity client for ChEMBL API."""

from __future__ import annotations

from bioetl.clients.base_entity import BaseEntityClient
from bioetl.clients.chembl_base import EntityConfig

__all__ = ["ChemblAssayEntityClient"]


class ChemblAssayEntityClient(BaseEntityClient):
    """Клиент для получения assay записей из ChEMBL API."""

    CONFIG = EntityConfig(
        endpoint="/assay.json",
        filter_param="assay_chembl_id__in",
        id_key="assay_chembl_id",
        items_key="assays",
        log_prefix="assay",
        chunk_size=100,
    )
