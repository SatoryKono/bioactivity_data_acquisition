"""Utilities for resolving lazily-imported attributes."""

from __future__ import annotations

from importlib import import_module
from typing import Any, Callable, Container, Mapping, MutableMapping

LazyMappingValue = str | tuple[str, str]
CachePolicy = bool | Container[str]


def _should_cache(name: str, policy: CachePolicy) -> bool:
    if policy is True:
        return True
    if policy is False:
        return False
    return name in policy


def resolve_lazy_attr(
    namespace: MutableMapping[str, Any],
    mapping: Mapping[str, LazyMappingValue],
    *,
    cache: CachePolicy = False,
) -> Callable[[str], Any]:
    """Create a lazy attribute resolver for module-level ``__getattr__`` hooks.

    Parameters
    ----------
    namespace:
        Mutable mapping representing the module namespace (e.g. ``globals()``).
    mapping:
        Mapping of attribute names to either a module path or explicit
        ``(module_path, attribute_name)`` tuple.
    cache:
        Defines whether resolved attributes should be cached in ``namespace``.
        Accepts ``True``/``False`` for all-or-nothing caching or a container of
        attribute names to cache selectively. Defaults to ``False``.
    """

    def loader(name: str) -> Any:
        target = mapping.get(name)
        if target is None:
            raise AttributeError(name)

        module_name: str
        attr_name: str
        if isinstance(target, tuple):
            module_name, attr_name = target
        else:
            module_name, attr_name = target, name

        module = import_module(module_name)
        value = getattr(module, attr_name)
        if _should_cache(name, cache):
            namespace[name] = value
        return value

    return loader
