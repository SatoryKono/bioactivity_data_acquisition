"""Adapters exposing the domain-friendly Chembl client protocol."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from .protocols import ChemblEntityClientProtocol


class ChemblAdapter(ChemblEntityClientProtocol):
    """Adapter bridging legacy Chembl clients to the domain protocol."""

    def __init__(self, low_level_client: Any) -> None:
        self._client = low_level_client

    def iterate_entities(
        self, ids: Iterable[str], *, select_fields: Iterable[str] | None = None
    ) -> Iterable[Mapping[str, Any]]:
        iterator = getattr(self._client, "iterate_by_ids", None)
        if callable(iterator):
            try:
                if select_fields is not None:
                    return iterator(ids, select_fields=select_fields)
            except TypeError:
                # Low-level client does not support select_fields; retry without it.
                pass
            return iterator(ids)

        fetch = getattr(self._client, "fetch_by_ids", None)
        if callable(fetch):
            try:
                result = (
                    fetch(list(ids), select_fields=select_fields)
                    if select_fields is not None
                    else fetch(list(ids))
                )
            except TypeError:
                result = fetch(list(ids))
            records = getattr(result, "frame", None)
            if records is not None and hasattr(records, "to_dict"):
                return records.to_dict("records")  # type: ignore[return-value]
            return result  # type: ignore[return-value]
        raise AttributeError("Low-level client does not support id iteration")

    def fetch_record(self, id: str) -> Mapping[str, Any]:
        iterator = self.iterate_entities([id])
        for record in iterator:
            return record
        return {}

    def iterate_by_ids(  # type: ignore[override]
        self, ids: Iterable[str], *, select_fields: Iterable[str] | None = None
    ) -> Iterable[Mapping[str, Any]]:
        """Compatibility shim preserving existing pipeline expectations."""

        return self.iterate_entities(ids, select_fields=select_fields)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._client, name)
