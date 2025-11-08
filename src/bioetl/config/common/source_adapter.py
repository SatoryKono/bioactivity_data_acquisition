"""Унифицированный адаптер конфигураций источников."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Protocol, TypeVar

from ..models.source import SourceConfig

AdapterT = TypeVar("AdapterT", bound="SourceConfigAdapter")


class SourceConfigAdapter(Protocol):
    """Контракт адаптера для типизированных SourceConfig обёрток."""

    @classmethod
    def from_source(cls: type[AdapterT], config: SourceConfig) -> AdapterT:
        """Создать адаптер из базовой конфигурации источника."""

    def enforce_limits(self: AdapterT) -> AdapterT:
        """Применить ограничители (например, капы размеров страниц)."""


def normalize_base_url(raw: Any) -> str | None:
    """Нормализовать base_url: trim + пустые в None."""

    if raw is None:
        return None
    candidate = str(raw).strip()
    return candidate or None


def normalize_select_fields(raw: Any) -> tuple[str, ...] | None:
    """Нормализовать select_fields в кортеж уникальных строк."""

    if raw is None:
        return None
    normalized: list[str] = []
    seen: set[str] = set()

    def _append(value: str) -> None:
        if value and value not in seen:
            normalized.append(value)
            seen.add(value)

    if isinstance(raw, str):
        for chunk in raw.split(","):
            _append(chunk.strip())
        return tuple(normalized) or None

    if isinstance(raw, Sequence) and not isinstance(raw, (bytes, bytearray, str)):
        for item in raw:
            if item is None:
                continue
            _append(str(item).strip())
        return tuple(normalized) or None

    return None


def extract_allowed_parameters(
    params: Mapping[str, Any],
    allowed_fields: Sequence[str],
) -> dict[str, Any]:
    """Отфильтровать параметры по whitelists, сохраняя порядок allowed_fields."""

    sanitized: dict[str, Any] = {}
    for field in allowed_fields:
        if field in params:
            sanitized[field] = params[field]
    return sanitized

