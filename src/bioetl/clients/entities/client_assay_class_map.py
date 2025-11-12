"""Chembl assay class map entity client."""

from __future__ import annotations

from typing import ClassVar

from bioetl.clients.client_base_entity import ChemblEntityClientBase
from bioetl.clients.client_chembl_base import EntityConfig, make_entity_config

__all__ = ["ChemblAssayClassMapEntityClient"]


class ChemblAssayClassMapEntityClient(ChemblEntityClientBase):
    """Клиент для получения assay_class_map записей из ChEMBL API."""

    CONFIG: ClassVar[EntityConfig] = make_entity_config(
        endpoint="/assay_class_map.json",
        filter_param="assay_chembl_id__in",
        id_key="assay_chembl_id",
        items_key="assay_class_maps",
        log_prefix="assay_class_map",
        chunk_size=100,
        supports_list_result=True,  # Один assay может иметь несколько class mappings
    )

