"""Chembl data validity entity client."""

from __future__ import annotations

from typing import ClassVar

from bioetl.clients.client_chembl_base import EntityConfig, make_entity_config
from bioetl.clients.client_chembl_entity import ChemblEntityClientBase

__all__ = ["ChemblDataValidityEntityClient"]


class ChemblDataValidityEntityClient(ChemblEntityClientBase):
    """Client for retrieving ``data_validity_lookup`` records from the ChEMBL API."""

    CONFIG: ClassVar[EntityConfig] = make_entity_config(
        endpoint="/data_validity_lookup.json",
        filter_param="data_validity_comment__in",
        id_key="data_validity_comment",
        items_key="data_validity_lookups",
        log_prefix="data_validity_lookup",
        chunk_size=100,
    )

