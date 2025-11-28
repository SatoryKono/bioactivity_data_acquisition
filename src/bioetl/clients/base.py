"""Базовые протоколы и реестр клиентских фабрик."""

from __future__ import annotations

from typing import Any, Protocol, TypeVar, runtime_checkable


@runtime_checkable
class ClientProtocol(Protocol):
    """Контракт для клиентских адаптеров, поддерживающих ``close``."""

    def close(self) -> None:  # pragma: no cover - протокол
        ...


T_co = TypeVar("T_co", covariant=True)


class ClientFactory(Protocol[T_co]):
    """Фабрика, создающая клиентов для сущностей домена."""

    def create(self, entity: str, mode: str | None = None) -> T_co:  # pragma: no cover - протокол
        ...


FACTORIES: dict[str, ClientFactory[Any]] = {}


def register_factory(name: str, factory: ClientFactory[Any]) -> None:
    """Зарегистрировать фабрику по имени домена."""

    FACTORIES[name] = factory


def get_factory(name: str) -> ClientFactory[Any]:
    """Получить фабрику по имени или выбросить ``KeyError``."""

    try:
        return FACTORIES[name]
    except KeyError as exc:  # pragma: no cover - defensive branch
        msg = f"Client factory '{name}' is not registered"
        raise KeyError(msg) from exc


def register_domain_factories(
    *,
    chembl_factory: ClientFactory[Any] | None = None,
    enricher_factory: ClientFactory[Any] | None = None,
) -> dict[str, ClientFactory[Any]]:
    """Утилита регистрации доменных фабрик по умолчанию."""

    if chembl_factory is not None:
        register_factory("chembl", chembl_factory)
    if enricher_factory is not None:
        register_factory("enricher", enricher_factory)
    return FACTORIES


__all__ = [
    "ClientProtocol",
    "ClientFactory",
    "FACTORIES",
    "register_factory",
    "register_domain_factories",
    "get_factory",
]
