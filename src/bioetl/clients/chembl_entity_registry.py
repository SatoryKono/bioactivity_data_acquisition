"""Декларативный реестр клиентов ChEMBL-сущностей."""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Callable, Mapping, MutableMapping, cast

from bioetl.clients.chembl_config import EntityConfig, get_entity_config
from bioetl.clients.client_chembl_base import ChemblClientProtocol
from bioetl.clients.entities.client_activity import ChemblActivityClient
from bioetl.clients.entities.client_assay import ChemblAssayClient
from bioetl.clients.entities.client_assay_class_map import ChemblAssayClassMapEntityClient
from bioetl.clients.entities.client_assay_classification import (
    ChemblAssayClassificationEntityClient,
)
from bioetl.clients.entities.client_assay_entity import ChemblAssayEntityClient
from bioetl.clients.entities.client_assay_parameters import ChemblAssayParametersEntityClient
from bioetl.clients.entities.client_compound_record import ChemblCompoundRecordEntityClient
from bioetl.clients.entities.client_data_validity import ChemblDataValidityEntityClient
from bioetl.clients.entities.client_document import ChemblDocumentClient
from bioetl.clients.entities.client_document_term import ChemblDocumentTermEntityClient
from bioetl.clients.entities.client_molecule import ChemblMoleculeEntityClient
from bioetl.clients.entities.client_target import ChemblTargetClient
from bioetl.clients.entities.client_testitem import ChemblTestitemClient
from bioetl.config.models.source import SourceConfig

__all__ = [
    "ChemblEntityBuilder",
    "ChemblEntityDefinition",
    "ChemblEntityRegistryError",
    "get_entity_definition",
    "iter_entity_definitions",
    "register_entity_definition",
]

ChemblEntityBuilder = Callable[[ChemblClientProtocol, SourceConfig | None, Mapping[str, Any] | None], Any]


class ChemblEntityRegistryError(LookupError):
    """Исключение при обращении к незарегистрированной сущности."""


@dataclass(frozen=True, slots=True)
class ChemblEntityDefinition:
    """Описатель ChEMBL-сущности в реестре."""

    name: str
    entity_config: EntityConfig | None
    build_client: ChemblEntityBuilder

    def __post_init__(self) -> None:
        if not self.name or not isinstance(self.name, str):
            msg = "name должен быть непустой строкой"
            raise ValueError(msg)
        normalized = self.name.strip()
        if normalized != self.name:
            msg = "name не должен содержать ведущих/замыкающих пробелов"
            raise ValueError(msg)


_REGISTRY: MutableMapping[str, ChemblEntityDefinition] = {}


def register_entity_definition(
    definition: ChemblEntityDefinition,
    *,
    replace: bool = False,
) -> None:
    """Добавить сущность в реестр."""

    key = definition.name
    if not replace and key in _REGISTRY:
        msg = f"Сущность '{key}' уже зарегистрирована"
        raise ValueError(msg)
    _REGISTRY[key] = definition


def get_entity_definition(name: str) -> ChemblEntityDefinition:
    """Вернуть описатель сущности по имени."""

    normalized = name.strip()
    try:
        return _REGISTRY[normalized]
    except KeyError as exc:  # pragma: no cover - защищаем публичный контракт
        msg = f"Сущность '{normalized}' не зарегистрирована в реестре"
        raise ChemblEntityRegistryError(msg) from exc


def iter_entity_definitions() -> Mapping[str, ChemblEntityDefinition]:
    """Вернуть неизменяемое представление реестра."""

    return MappingProxyType(dict(_REGISTRY))


# ---------------------------------------------------------------------------
# Помощники для построения клиентов
# ---------------------------------------------------------------------------


def _extract_mapping_value(
    mapping: Mapping[str, Any] | None,
    key: str,
) -> Any | None:
    if mapping is None:
        return None
    if key in mapping:
        return mapping[key]
    return None


def _resolve_positive_int(
    value: Any | None,
    *,
    field_name: str,
    minimum: int = 1,
    maximum: int | None = None,
    allow_none: bool = False,
) -> int | None:
    if value is None:
        if allow_none:
            return None
        msg = f"{field_name} не найден"
        raise ValueError(msg)
    if isinstance(value, bool):
        msg = f"{field_name} не может быть логическим значением"
        raise TypeError(msg)
    try:
        candidate = int(value)
    except (TypeError, ValueError) as exc:
        msg = f"{field_name} должен приводиться к целому числу"
        raise TypeError(msg) from exc
    if candidate < minimum:
        msg = f"{field_name} должен быть ≥ {minimum}"
        raise ValueError(msg)
    if maximum is not None and candidate > maximum:
        candidate = maximum
    return candidate


def _extract_source_parameters(source: SourceConfig | None) -> Mapping[str, Any]:
    if source is None:
        return {}
    return source.parameters_mapping()


def _resolve_batch_size(
    source: SourceConfig | None,
    overrides: Mapping[str, Any] | None,
    *,
    default: int = 25,
) -> int:
    candidate = _extract_mapping_value(overrides, "batch_size")
    if candidate is None and source is not None:
        candidate = getattr(source, "batch_size", None)
    if candidate is None and source is not None:
        parameters = _extract_source_parameters(source)
        candidate = _extract_mapping_value(parameters, "batch_size")
    resolved = _resolve_positive_int(
        candidate if candidate is not None else default,
        field_name="batch_size",
        minimum=1,
        maximum=25,
    )
    return cast(int, resolved)


def _resolve_max_url_length(
    source: SourceConfig | None,
    overrides: Mapping[str, Any] | None,
    *,
    allow_none: bool = True,
    default: int | None = None,
) -> int | None:
    candidate = _extract_mapping_value(overrides, "max_url_length")
    if candidate is None and source is not None:
        candidate = getattr(source, "max_url_length", None)
    if candidate is None and source is not None:
        parameters = _extract_source_parameters(source)
        candidate = _extract_mapping_value(parameters, "max_url_length")
    if candidate is None:
        return default
    return _resolve_positive_int(
        candidate,
        field_name="max_url_length",
        minimum=1,
        allow_none=allow_none,
    )


def _iterator_builder(
    iterator_cls: type[Any],
    *,
    require_max_url_length: bool = False,
    default_batch_size: int = 25,
    default_max_url_length: int | None = None,
) -> ChemblEntityBuilder:
    def _builder(
        chembl_client: ChemblClientProtocol,
        source_config: SourceConfig | None,
        options: Mapping[str, Any] | None,
    ) -> Any:
        batch_size = _resolve_batch_size(
            source_config,
            options,
            default=default_batch_size,
        )
        max_url_length = _resolve_max_url_length(
            source_config,
            options,
            allow_none=not require_max_url_length,
            default=default_max_url_length,
        )
        kwargs: dict[str, Any] = {"batch_size": batch_size}
        if require_max_url_length or max_url_length is not None:
            if require_max_url_length and max_url_length is None:
                msg = "max_url_length обязателен для данной сущности"
                raise ValueError(msg)
            kwargs["max_url_length"] = max_url_length
        return iterator_cls(chembl_client, **kwargs)

    return _builder


def _simple_builder(client_cls: type[Any]) -> ChemblEntityBuilder:
    def _builder(
        chembl_client: ChemblClientProtocol,
        _: SourceConfig | None,
        __: Mapping[str, Any] | None,
    ) -> Any:
        return client_cls(chembl_client)

    return _builder


# ---------------------------------------------------------------------------
# Статическая регистрация сущностей ChEMBL
# ---------------------------------------------------------------------------

register_entity_definition(
    ChemblEntityDefinition(
        name="activity",
        entity_config=get_entity_config("activity"),
        build_client=_iterator_builder(
            iterator_cls=ChemblActivityClient,
        ),
    )
)

register_entity_definition(
    ChemblEntityDefinition(
        name="assay",
        entity_config=get_entity_config("assay"),
        build_client=_iterator_builder(
            iterator_cls=ChemblAssayClient,
            require_max_url_length=True,
        ),
    )
)

register_entity_definition(
    ChemblEntityDefinition(
        name="target",
        entity_config=get_entity_config("target"),
        build_client=_iterator_builder(iterator_cls=ChemblTargetClient),
    )
)

register_entity_definition(
    ChemblEntityDefinition(
        name="document",
        entity_config=get_entity_config("document"),
        build_client=_iterator_builder(iterator_cls=ChemblDocumentClient),
    )
)

register_entity_definition(
    ChemblEntityDefinition(
        name="testitem",
        entity_config=get_entity_config("testitem"),
        build_client=_iterator_builder(iterator_cls=ChemblTestitemClient),
    )
)

register_entity_definition(
    ChemblEntityDefinition(
        name="assay_classification",
        entity_config=get_entity_config("assay_classification"),
        build_client=_simple_builder(ChemblAssayClassificationEntityClient),
    )
)

register_entity_definition(
    ChemblEntityDefinition(
        name="assay_class_map",
        entity_config=get_entity_config("assay_class_map"),
        build_client=_simple_builder(ChemblAssayClassMapEntityClient),
    )
)

register_entity_definition(
    ChemblEntityDefinition(
        name="assay_parameters",
        entity_config=get_entity_config("assay_parameters"),
        build_client=_simple_builder(ChemblAssayParametersEntityClient),
    )
)

register_entity_definition(
    ChemblEntityDefinition(
        name="data_validity_lookup",
        entity_config=get_entity_config("data_validity_lookup"),
        build_client=_simple_builder(ChemblDataValidityEntityClient),
    )
)

register_entity_definition(
    ChemblEntityDefinition(
        name="data_validity",
        entity_config=get_entity_config("data_validity_lookup"),
        build_client=_simple_builder(ChemblDataValidityEntityClient),
    )
)

register_entity_definition(
    ChemblEntityDefinition(
        name="document_term",
        entity_config=get_entity_config("document_term"),
        build_client=_simple_builder(ChemblDocumentTermEntityClient),
    )
)

register_entity_definition(
    ChemblEntityDefinition(
        name="molecule",
        entity_config=get_entity_config("molecule"),
        build_client=_simple_builder(ChemblMoleculeEntityClient),
    )
)

register_entity_definition(
    ChemblEntityDefinition(
        name="assay_entity",
        entity_config=get_entity_config("assay"),
        build_client=_simple_builder(ChemblAssayEntityClient),
    )
)

register_entity_definition(
    ChemblEntityDefinition(
        name="compound_record",
        entity_config=None,
        build_client=_simple_builder(ChemblCompoundRecordEntityClient),
    )
)
