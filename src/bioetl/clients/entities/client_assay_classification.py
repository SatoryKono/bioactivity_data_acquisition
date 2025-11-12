"""Chembl assay classification entity client."""

from __future__ import annotations

from typing import ClassVar

from bioetl.clients.client_base_entity import ChemblEntityClientBase
from bioetl.clients.client_chembl_base import EntityConfig, make_entity_config

__all__ = ["ChemblAssayClassificationEntityClient"]


class ChemblAssayClassificationEntityClient(ChemblEntityClientBase):
    """Клиент для получения assay_classification записей из ChEMBL API."""

    CONFIG: ClassVar[EntityConfig] = make_entity_config(
        endpoint="/assay_classification.json",
        filter_param="assay_class_id__in",
        id_key="assay_class_id",
        items_key="assay_classifications",
        log_prefix="assay_classification",
        chunk_size=100,
    )

