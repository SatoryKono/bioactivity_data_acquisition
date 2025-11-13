"""Chembl assay classification entity client."""

from __future__ import annotations

from typing import ClassVar

from bioetl.clients.client_chembl_base import EntityConfig, make_entity_config
from bioetl.clients.client_chembl_entity import ChemblEntityClientBase

__all__ = ["ChemblAssayClassificationEntityClient"]


class ChemblAssayClassificationEntityClient(ChemblEntityClientBase):
    """Client for retrieving ``assay_classification`` records from the ChEMBL API."""

    CONFIG: ClassVar[EntityConfig] = make_entity_config(
        endpoint="/assay_classification.json",
        filter_param="assay_class_id__in",
        id_key="assay_class_id",
        items_key="assay_classifications",
        log_prefix="assay_classification",
        chunk_size=100,
    )

