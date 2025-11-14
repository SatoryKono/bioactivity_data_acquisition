"""Offline stub for ``chembl_webresource_client.new_client``.

Provide a deterministic client mirroring the minimal interface surface of
``chembl_webresource_client``. Used in tests and offline scenarios where network
access is prohibited.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass

__all__ = [
    "OfflineChemblClient",
    "get_offline_new_client",
]


_ActivityRecord = Mapping[str, object]


@dataclass(frozen=True)
class _OfflineQuery:
    """Lazy result emulating the ChEMBL client's ``filter`` query."""

    _rows: Sequence[Mapping[str, object]]

    def only(self, field: str) -> "_OfflineQuery":
        trimmed: list[Mapping[str, object]] = []
        for row in self._rows:
            trimmed.append({field: row.get(field)})
        return _OfflineQuery(trimmed)

    def __iter__(self) -> Iterator[Mapping[str, object]]:
        return iter(self._rows)


@dataclass(frozen=True)
class _OfflineResource:
    """Minimal resource supporting ``filter`` and pagination offsets."""

    _records: Sequence[Mapping[str, object]]

    def filter(self, **filters: object) -> _OfflineQuery:
        limit = _safe_int(filters.get("limit"), len(self._records))
        offset = _safe_int(filters.get("offset"), 0)
        sliced = self._records[offset : offset + limit]
        return _OfflineQuery(list(sliced))


@dataclass
class OfflineChemblClient:
    """Stubbed client exposing the limited resources needed offline."""

    activity: _OfflineResource
    assay: _OfflineResource
    target: _OfflineResource
    data_validity_lookup: _OfflineResource
    mechanism: _OfflineResource


_OFFLINE_DATA: dict[str, list[_ActivityRecord]] = {
    "activity": [
        {
            "standard_type": "IC50",
            "standard_units": "nM",
            "standard_relation": "=",
            "bao_format": "BAO_0000015",
        }
    ],
    "assay": [
        {
            "assay_type": "B",
        }
    ],
    "target": [
        {
            "target_type": "SINGLE PROTEIN",
        }
    ],
    "data_validity_lookup": [
        {
            "data_validity_comment": "Manually validated",
        }
    ],
    "mechanism": [
        {
            "action_type": "INHIBITOR",
        }
    ],
}


def get_offline_new_client() -> OfflineChemblClient:
    """Return the deterministic offline ChEMBL client."""

    return OfflineChemblClient(
        activity=_OfflineResource(_OFFLINE_DATA["activity"]),
        assay=_OfflineResource(_OFFLINE_DATA["assay"]),
        target=_OfflineResource(_OFFLINE_DATA["target"]),
        data_validity_lookup=_OfflineResource(_OFFLINE_DATA["data_validity_lookup"]),
        mechanism=_OfflineResource(_OFFLINE_DATA["mechanism"]),
    )


def _safe_int(value: object, default: int) -> int:
    """Coerce a value to ``int`` with fallback to ``default``."""
    if isinstance(value, bool):  # bool is subclass of int, treat separately
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped:
            try:
                return int(stripped)
            except ValueError:
                return default
    return default


