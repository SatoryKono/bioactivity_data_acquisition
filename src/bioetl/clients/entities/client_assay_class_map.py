"""Chembl assay class map entity client."""

from __future__ import annotations

from typing import ClassVar

from bioetl.clients.client_chembl_base import EntityConfig, make_entity_config
from bioetl.clients.client_chembl_entity import ChemblEntityClientBase

__all__ = ["ChemblAssayClassMapEntityClient"]


class ChemblAssayClassMapEntityClient(ChemblEntityClientBase):
    """Client for retrieving ``assay_class_map`` records from the ChEMBL API."""

    CONFIG: ClassVar[EntityConfig] = make_entity_config(
        endpoint="/assay_class_map.json",
        filter_param="assay_chembl_id__in",
        id_key="assay_chembl_id",
        items_key="assay_class_maps",
        log_prefix="assay_class_map",
        chunk_size=100,
        supports_list_result=True,  # A single assay may expose multiple class mappings
    )

