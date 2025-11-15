from __future__ import annotations

import sys
from types import ModuleType
from typing import Callable, Iterator

import pytest

from bioetl.core.runtime.lazy_loader import resolve_lazy_attr


@pytest.fixture
def fake_module() -> Iterator[Callable[[str, dict[str, object]], ModuleType]]:
    created: list[str] = []

    def factory(name: str, attributes: dict[str, object]) -> ModuleType:
        module = ModuleType(name)
        for attr_name, value in attributes.items():
            setattr(module, attr_name, value)
        sys.modules[name] = module
        created.append(name)
        return module

    try:
        yield factory
    finally:
        for module_name in created:
            sys.modules.pop(module_name, None)


def test_resolve_lazy_attr_without_cache(fake_module: Callable[[str, dict[str, object]], ModuleType]) -> None:
    module_name = "tests.runtime.lazy_loader.no_cache"
    sentinel = object()
    fake_module(module_name, {"Foo": sentinel})

    namespace: dict[str, object] = {}
    resolver = resolve_lazy_attr(namespace, {"Foo": module_name}, cache=False)

    assert resolver("Foo") is sentinel
    assert "Foo" not in namespace


def test_resolve_lazy_attr_with_cache(fake_module: Callable[[str, dict[str, object]], ModuleType]) -> None:
    module_name = "tests.runtime.lazy_loader.cache"
    sentinel = object()
    fake_module(module_name, {"Bar": sentinel})

    namespace: dict[str, object] = {}
    resolver = resolve_lazy_attr(namespace, {"Bar": module_name}, cache=True)

    assert resolver("Bar") is sentinel
    assert namespace["Bar"] is sentinel


def test_resolve_lazy_attr_with_alias_and_selective_cache(
    fake_module: Callable[[str, dict[str, object]], ModuleType]
) -> None:
    module_name = "tests.runtime.lazy_loader.alias"
    sentinel = object()
    fake_module(module_name, {"Target": sentinel, "ByName": sentinel})

    namespace: dict[str, object] = {}
    resolver = resolve_lazy_attr(
        namespace,
        {
            "Alias": (module_name, "Target"),
            "ByName": module_name,
        },
        cache={"Alias"},
    )

    assert resolver("Alias") is sentinel
    assert namespace["Alias"] is sentinel

    assert resolver("ByName") is sentinel
    assert "ByName" not in namespace


def test_resolve_lazy_attr_unknown_name(fake_module: Callable[[str, dict[str, object]], ModuleType]) -> None:
    module_name = "tests.runtime.lazy_loader.unknown"
    fake_module(module_name, {"Foo": object()})

    namespace: dict[str, object] = {}
    resolver = resolve_lazy_attr(namespace, {"Foo": module_name})

    with pytest.raises(AttributeError):
        resolver("Bar")
