"""Chembl data validity entity client."""

from __future__ import annotations

from typing import ClassVar

from bioetl.clients.client_base_entity import ChemblEntityClientBase
from bioetl.clients.client_chembl_base import EntityConfig, make_entity_config

__all__ = ["ChemblDataValidityEntityClient"]


class ChemblDataValidityEntityClient(ChemblEntityClientBase):
    """Клиент для получения data_validity_lookup записей из ChEMBL API."""

    CONFIG: ClassVar[EntityConfig] = make_entity_config(
        endpoint="/data_validity_lookup.json",
        filter_param="data_validity_comment__in",
        id_key="data_validity_comment",
        items_key="data_validity_lookups",
        log_prefix="data_validity_lookup",
        chunk_size=100,
    )

