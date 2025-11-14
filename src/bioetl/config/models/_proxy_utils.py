"""Shared helpers for generating proxy properties."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping, Sequence


@dataclass(slots=True, frozen=True)
class ProxyDefinition:
    """Declarative binding between attribute name and nested path."""

    attr: str
    path: tuple[str, ...]
    return_type: Any | None = None
    value_type: Any | None = None
    setter: Callable[[Any], Any] | None = None
    read_only: bool | None = None


def _coerce_to_tuple(path: Sequence[str] | str) -> tuple[str, ...]:
    if isinstance(path, str):
        return tuple(path.split("."))
    return tuple(path)


def _build_property(
    *,
    path: tuple[str, ...],
    setter: Callable[[Any], Any] | None,
    read_only: bool,
    return_type: Any | None,
    value_type: Any | None,
) -> property:
    value_annotation = value_type if value_type is not None else return_type

    def getter(instance: Any) -> Any:
        target = instance
        for attr in path:
            target = getattr(target, attr)
        return target

    if return_type is not None:
        getter.__annotations__["return"] = return_type

    if read_only:
        return property(getter)

    def setter_fn(instance: Any, value: Any) -> None:
        target = instance
        *parents, leaf = path
        for attr in parents:
            target = getattr(target, attr)
        if setter is not None:
            value = setter(value)
        setattr(target, leaf, value)

    if value_annotation is not None:
        setter_fn.__annotations__["value"] = value_annotation

    return property(getter, setter_fn)


def build_section_proxies(
    definitions: Iterable[ProxyDefinition] | Mapping[str, Sequence[str] | str],
    *,
    default_read_only: bool = False,
) -> dict[str, property]:
    """Create properties that proxy to nested attributes."""

    if isinstance(definitions, Mapping):
        parsed = (
            ProxyDefinition(attr=attr, path=_coerce_to_tuple(path))
            for attr, path in definitions.items()
        )
    else:
        parsed = definitions

    proxies: dict[str, property] = {}
    for definition in parsed:
        read_only = default_read_only if definition.read_only is None else definition.read_only
        proxies[definition.attr] = _build_property(
            path=definition.path,
            setter=definition.setter,
            read_only=read_only,
            return_type=definition.return_type,
            value_type=definition.value_type,
        )
    return proxies
