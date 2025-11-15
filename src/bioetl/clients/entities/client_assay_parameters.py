"""Chembl assay parameters entity client."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any, ClassVar

import pandas as pd

from bioetl.clients.chembl_config import EntityConfig, get_entity_config
from bioetl.clients.client_chembl_entity_base import (
    ChemblEntityConfigMixin,
    ChemblEntityFetcherBase,
)

__all__ = ["ChemblAssayParametersEntityClient"]


class ChemblAssayParametersEntityClient(
    ChemblEntityConfigMixin, ChemblEntityFetcherBase
):
    """Client for retrieving ``assay_parameters`` records from the ChEMBL API."""

    ENTITY_CONFIG: ClassVar[EntityConfig] = get_entity_config("assay_parameters")
    _ACTIVE_ONLY_DEFAULT = True
    _active_only_current: bool = _ACTIVE_ONLY_DEFAULT

    def fetch_by_ids(
        self,
        ids: Iterable[str],
        fields: Sequence[str] | None = None,
        *,
        page_limit: int | None = None,
        active_only: bool = True,
    ) -> pd.DataFrame:
        """Retrieve ``assay_parameters`` records by assay identifiers."""
        identifiers = tuple(ids)
        previous = getattr(self, "_active_only_current", self._ACTIVE_ONLY_DEFAULT)
        self._active_only_current = active_only
        try:
            return super().fetch_by_ids(
                identifiers,
                fields=fields,
                page_limit=page_limit,
            )
        finally:
            self._active_only_current = previous

    def _build_chunk_params(
        self,
        chunk: Sequence[str],
        *,
        fields: Sequence[str] | None,
    ) -> dict[str, Any]:
        params = super()._build_chunk_params(chunk, fields=fields)
        if getattr(self, "_active_only_current", self._ACTIVE_ONLY_DEFAULT):
            params["active"] = "1"
        return params

