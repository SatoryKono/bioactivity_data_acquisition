"""Utilities for building deprecated compatibility shims."""

from __future__ import annotations

from importlib import import_module
from typing import Mapping, Sequence
from warnings import warn


def _iter_exports(module: object) -> Sequence[str]:
    exported = getattr(module, "__all__", None)
    if exported is None:
        return [name for name in dir(module) if not name.startswith("_")]
    return list(exported)


def reexport(
    old_name: str,
    new_name: str,
    namespace: dict[str, object],
    *,
    aliases: Mapping[str, str] | None = None,
) -> None:
    """Populate ``namespace`` with objects from ``new_name``.

    Parameters
    ----------
    old_name:
        Fully qualified name of the deprecated module.
    new_name:
        Fully qualified name of the replacement module.
    namespace:
        The ``globals()`` dictionary of the compatibility module.
    aliases:
        Mapping of attribute name to fetch from the new module and expose under
        a different legacy name.
    """

    warn(
        f"`{old_name}` is deprecated and will be removed in a future release; use `{new_name}` instead.",
        DeprecationWarning,
        stacklevel=3,
    )
    target = import_module(new_name)

    exported = list(_iter_exports(target))
    namespace.update({name: getattr(target, name) for name in exported})

    resolved_aliases: dict[str, object] = {}
    if aliases:
        for legacy_name, current_name in aliases.items():
            resolved_aliases[legacy_name] = getattr(target, current_name)
        namespace.update(resolved_aliases)
        exported.extend(resolved_aliases)

    namespace["__all__"] = sorted(set(exported))
    namespace.setdefault("__doc__", getattr(target, "__doc__", None))

    target_spec = getattr(target, "__spec__", None)
    if target_spec and getattr(target_spec, "submodule_search_locations", None):
        namespace["__path__"] = list(target_spec.submodule_search_locations)
        spec = namespace.get("__spec__")
        if spec is not None:
            spec.submodule_search_locations = list(target_spec.submodule_search_locations)
    elif hasattr(target, "__path__"):
        namespace["__path__"] = list(getattr(target, "__path__"))  # type: ignore[attr-defined]

    def __getattr__(name: str) -> object:
        if aliases and name in resolved_aliases:
            return resolved_aliases[name]
        return getattr(target, name)

    def __dir__() -> list[str]:
        return sorted(set(namespace["__all__"]) | set(dir(target)))

    namespace["__getattr__"] = __getattr__
    namespace["__dir__"] = __dir__
    namespace["_compat_target"] = target

