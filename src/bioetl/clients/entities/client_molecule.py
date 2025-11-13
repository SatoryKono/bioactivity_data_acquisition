"""Chembl molecule entity client."""

from __future__ import annotations

from typing import ClassVar

from bioetl.clients.client_chembl_base import EntityConfig, make_entity_config
from bioetl.clients.client_chembl_entity import ChemblEntityClientBase

__all__ = ["ChemblMoleculeEntityClient"]


class ChemblMoleculeEntityClient(ChemblEntityClientBase):
    """Клиент для получения molecule записей из ChEMBL API."""

    CONFIG: ClassVar[EntityConfig] = make_entity_config(
        endpoint="/molecule.json",
        filter_param="molecule_chembl_id__in",
        id_key="molecule_chembl_id",
        items_key="molecules",
        log_prefix="molecule",
        chunk_size=100,
    )

