"""Thin parser adapting ChEMBL activity payloads to the shared contract."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import Any

from bioetl.base_classes import IParser

__all__ = ["ChemblActivityParser"]


class ChemblActivityParser(IParser):
    """Convert raw ChEMBL API payloads into iterable activity records."""

    def __init__(self, *, items_key: str = "activities") -> None:
        self._items_key = items_key

    def parse(self, raw: Any) -> Iterable[dict[str, Any]]:
        """Return iterable dictionaries extracted from ``raw`` payloads."""

        return tuple(self._iter_records(raw))

    def _iter_records(self, raw: Any) -> Iterator[dict[str, Any]]:
        if raw is None:
            return
        if isinstance(raw, Mapping):
            candidate = raw.get(self._items_key)
            if candidate is None:
                candidate = raw.get("items")
            if candidate is not None:
                yield from self._normalize_iterable(candidate)
                return
            # Treat mapping without nested items as a single record.
            yield self._coerce_mapping(raw)
            return
        if isinstance(raw, Sequence) and not isinstance(
            raw, (str, bytes, bytearray)
        ):
            yield from self._normalize_iterable(raw)
            return
        if isinstance(raw, Iterable) and not isinstance(
            raw, (str, bytes, bytearray)
        ):
            yield from self._normalize_iterable(raw)

    def _normalize_iterable(self, records: Any) -> Iterator[dict[str, Any]]:
        if records is None:
            return
        if isinstance(records, Mapping):
            yield self._coerce_mapping(records)
            return
        if isinstance(records, Sequence) and not isinstance(
            records, (str, bytes, bytearray)
        ):
            for record in records:
                yield from self._normalize_iterable(record)
            return
        if isinstance(records, Iterable) and not isinstance(
            records, (str, bytes, bytearray)
        ):
            for record in records:
                yield from self._normalize_iterable(record)

    @staticmethod
    def _coerce_mapping(record: Mapping[str, Any]) -> dict[str, Any]:
        return {str(key): value for key, value in record.items()}
