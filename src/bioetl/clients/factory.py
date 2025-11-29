from __future__ import annotations

from typing import Callable, Dict

from bioetl.clients.base.client import DataClient

ClientFactory = Callable[[str], DataClient]

_FACTORIES: Dict[str, ClientFactory] = {}


def register_factory(name: str, factory: ClientFactory) -> None:
    """
    Зарегистрировать фабрику клиентов для источника (например, "chembl").
    """

    _FACTORIES[name] = factory


def get_factory(name: str) -> ClientFactory:
    """
    Получить фабрику по имени источника.
    """

    try:
        return _FACTORIES[name]
    except KeyError as exc:
        msg = f"Client factory '{name}' is not registered"
        raise KeyError(msg) from exc


def get_client(source: str, client_name: str) -> DataClient:
    """
    Удобный доступ: получить клиента по источнику и имени клиента.
    Эквивалент get_factory(source)(client_name).
    """

    factory = get_factory(source)
    return factory(client_name)
