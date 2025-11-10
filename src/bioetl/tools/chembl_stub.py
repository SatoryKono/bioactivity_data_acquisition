"""Offline stub for ``chembl_webresource_client.new_client``.

Модуль предоставляет детерминированный клиент, имитирующий интерфейс
``chembl_webresource_client`` с минимальным подмножеством данных. Он
используется в тестах и офлайн-сценариях, где сетевые обращения запрещены.
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
class OfflineQuery:
    """Имитация ленивого результата ``.filter()`` ChEMBL клиента."""

    _rows: Sequence[Mapping[str, object]]

    def only(self, field: str) -> OfflineQuery:
        trimmed: list[Mapping[str, object]] = []
        for row in self._rows:
            trimmed.append({field: row.get(field)})
        return OfflineQuery(trimmed)

    def __iter__(self) -> Iterator[Mapping[str, object]]:
        return iter(self._rows)


@dataclass(frozen=True)
class OfflineResource:
    """Простейший ресурс с поддержкой ``filter`` и пагинации."""

    _records: Sequence[Mapping[str, object]]

    def filter(self, **filters: object) -> OfflineQuery:
        limit = _safe_int(filters.get("limit"), len(self._records))
        offset = _safe_int(filters.get("offset"), 0)
        sliced = self._records[offset : offset + limit]
        return OfflineQuery(list(sliced))


@dataclass
class OfflineChemblClient:
    """Фиктивный клиент с минимально необходимыми ресурсами."""

    activity: OfflineResource
    assay: OfflineResource
    target: OfflineResource
    data_validity_lookup: OfflineResource
    mechanism: OfflineResource


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
    """Вернуть детерминированный офлайн-клиент."""

    return OfflineChemblClient(
        activity=OfflineResource(_OFFLINE_DATA["activity"]),
        assay=OfflineResource(_OFFLINE_DATA["assay"]),
        target=OfflineResource(_OFFLINE_DATA["target"]),
        data_validity_lookup=OfflineResource(_OFFLINE_DATA["data_validity_lookup"]),
        mechanism=OfflineResource(_OFFLINE_DATA["mechanism"]),
    )


def _safe_int(value: object, default: int) -> int:
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


