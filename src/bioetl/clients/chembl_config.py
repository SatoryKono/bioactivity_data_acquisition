"""Декларативные конфигурации для ChEMBL-сущностей."""

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Callable, Iterator, Mapping, Sequence

__all__ = [
    "DedupStrategy",
    "EntityConfig",
    "EntityConfigRegistry",
    "ENTITY_CONFIG_REGISTRY",
    "register_entity_config",
    "get_entity_config",
]

DedupStrategy = Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]]


def _ensure_non_empty(value: str, *, field_name: str) -> str:
    if not isinstance(value, str):
        msg = f"{field_name} должен быть строкой, получено {type(value)!r}"
        raise TypeError(msg)
    normalized = value.strip()
    if not normalized:
        msg = f"{field_name} не может быть пустым"
        raise ValueError(msg)
    return normalized


def _normalize_field_names(fields: Sequence[str]) -> tuple[str, ...]:
    normalized: list[str] = []
    for candidate in fields:
        if not isinstance(candidate, str):
            msg = (
                "default_fields и ordering должны содержать только строки, "
                f"найдено {type(candidate)!r}"
            )
            raise TypeError(msg)
        field_name = candidate.strip()
        if not field_name:
            msg = "Имена полей не могут быть пустыми"
            raise ValueError(msg)
        normalized.append(field_name)
    return tuple(normalized)


def _normalize_filters(filters: Mapping[str, Any]) -> Mapping[str, Any]:
    items: list[tuple[str, Any]] = []
    for key, value in filters.items():
        if not isinstance(key, str):
            msg = "Ключи фильтров должны быть строками"
            raise TypeError(msg)
        normalized_key = key.strip()
        if not normalized_key:
            msg = "Ключ фильтра не может быть пустым"
            raise ValueError(msg)
        items.append((normalized_key, value))
    # Сортировка обеспечивает детерминированность порядка ключей.
    sorted_items = sorted(items, key=lambda pair: pair[0])
    return MappingProxyType(dict(sorted_items))


@dataclass(frozen=True, slots=True)
class EntityConfig:
    """Определяет контракт доступа к конкретной ChEMBL-сущности."""

    endpoint: str
    id_field: str
    filter_param: str
    items_key: str
    log_prefix: str
    default_fields: Sequence[str] = field(default_factory=tuple)
    supports_list_result: bool = False
    chunk_size: int = 100
    max_page_size: int | None = None
    ordering: Sequence[str] = field(default_factory=tuple)
    filters: Mapping[str, Any] = field(default_factory=dict)
    base_endpoint_length: int | None = None
    enable_url_length_check: bool = False
    dedup_priority: DedupStrategy | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "endpoint",
            _ensure_non_empty(self.endpoint, field_name="endpoint"),
        )
        object.__setattr__(
            self,
            "id_field",
            _ensure_non_empty(self.id_field, field_name="id_field"),
        )
        object.__setattr__(
            self,
            "filter_param",
            _ensure_non_empty(self.filter_param, field_name="filter_param"),
        )
        object.__setattr__(
            self,
            "items_key",
            _ensure_non_empty(self.items_key, field_name="items_key"),
        )
        object.__setattr__(
            self,
            "log_prefix",
            _ensure_non_empty(self.log_prefix, field_name="log_prefix"),
        )

        if isinstance(self.default_fields, tuple):
            default_fields = self.default_fields
        else:
            default_fields = tuple(self.default_fields)
        object.__setattr__(
            self,
            "default_fields",
            _normalize_field_names(default_fields),
        )

        if isinstance(self.ordering, tuple):
            ordering = self.ordering
        else:
            ordering = tuple(self.ordering)
        object.__setattr__(
            self,
            "ordering",
            _normalize_field_names(ordering),
        )

        if not isinstance(self.chunk_size, int) or isinstance(self.chunk_size, bool):
            msg = f"chunk_size должен быть целым числом, получено {self.chunk_size!r}"
            raise TypeError(msg)
        if self.chunk_size <= 0:
            msg = "chunk_size должен быть положительным"
            raise ValueError(msg)

        if self.max_page_size is not None:
            if not isinstance(self.max_page_size, int) or isinstance(
                self.max_page_size, bool
            ):
                msg = (
                    f"max_page_size должен быть целым числом, "
                    f"получено {self.max_page_size!r}"
                )
                raise TypeError(msg)
            if self.max_page_size <= 0:
                msg = "max_page_size должен быть положительным, если задан"
                raise ValueError(msg)

        if not isinstance(self.filters, Mapping):
            msg = "filters должен быть отображением (Mapping)"
            raise TypeError(msg)
        object.__setattr__(self, "filters", _normalize_filters(self.filters))

    def iter_default_filters(self) -> Iterator[tuple[str, Any]]:
        """Итератор по фильтрам по умолчанию в детерминированном порядке."""

        for item in self.filters.items():
            yield item


class EntityConfigRegistry:
    """Реестр конфигураций ChEMBL-сущностей."""

    def __init__(self) -> None:
        self._configs: dict[str, EntityConfig] = {}

    def register(
        self,
        name: str,
        config: EntityConfig,
        *,
        replace: bool = False,
    ) -> None:
        normalized = _ensure_non_empty(name, field_name="name")
        if not replace and normalized in self._configs:
            msg = f"Конфигурация '{normalized}' уже зарегистрирована"
            raise ValueError(msg)
        self._configs[normalized] = config

    def get(self, name: str) -> EntityConfig:
        normalized = _ensure_non_empty(name, field_name="name")
        try:
            return self._configs[normalized]
        except KeyError as exc:
            msg = f"Конфигурация '{normalized}' не найдена"
            raise KeyError(msg) from exc

    def contains(self, name: str) -> bool:
        normalized = name.strip()
        if not normalized:
            return False
        return normalized in self._configs

    def as_mapping(self) -> Mapping[str, EntityConfig]:
        return MappingProxyType(dict(self._configs))


ENTITY_CONFIG_REGISTRY = EntityConfigRegistry()


def register_entity_config(
    name: str,
    config: EntityConfig,
    *,
    replace: bool = False,
) -> None:
    ENTITY_CONFIG_REGISTRY.register(name, config, replace=replace)


def get_entity_config(name: str) -> EntityConfig:
    return ENTITY_CONFIG_REGISTRY.get(name)

