"""Minimal stub of the ``deprecated`` package used for testing."""

from __future__ import annotations

from typing import Any, Callable, TypeVar

F = TypeVar("F", bound=Callable[..., Any])


def deprecated(*args: Any, **kwargs: Any):  # type: ignore[override]
    if args and callable(args[0]) and len(args) == 1 and not kwargs:
        return args[0]

    def decorator(func: F) -> F:
        return func

    return decorator


def deprecated_class(*args: Any, **kwargs: Any):  # type: ignore[override]
    if args and isinstance(args[0], type) and len(args) == 1 and not kwargs:
        return args[0]

    def decorator(cls: type) -> type:
        return cls

    return decorator


__all__ = ["deprecated", "deprecated_class"]
