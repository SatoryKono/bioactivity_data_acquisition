"""Assay entity client for ChEMBL API."""

from __future__ import annotations

from typing import ClassVar

from bioetl.clients.client_chembl_base import EntityConfig, make_entity_config
from bioetl.clients.client_chembl_entity import ChemblEntityClientBase

__all__ = ["ChemblAssayEntityClient"]


class ChemblAssayEntityClient(ChemblEntityClientBase):
    """Client for fetching assay entries from the ChEMBL API."""

    CONFIG: ClassVar[EntityConfig] = make_entity_config(
        endpoint="/assay.json",
        filter_param="assay_chembl_id__in",
        id_key="assay_chembl_id",
        items_key="assays",
        log_prefix="assay",
        chunk_size=100,
        base_endpoint_length=len("/assay.json?"),
        enable_url_length_check=True,
    )

