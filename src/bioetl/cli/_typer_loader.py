"""Utilities for loading the optional :mod:`typer` dependency."""

from __future__ import annotations

import importlib
from collections.abc import Callable
from typing import Any, Protocol, cast

__all__ = ["TyperModule", "_load_typer"]


class TyperModule(Protocol):
    """Minimal contract of the ``typer`` module required by this package."""

    Typer: Callable[..., Any]
    Option: Callable[..., Any]
    echo: Callable[..., None]
    secho: Callable[..., None]
    colors: Any


_typer_module: TyperModule | None = None


def _load_typer() -> TyperModule:
    """Import ``typer`` and raise a descriptive error when the dependency is missing."""

    global _typer_module
    if _typer_module is not None:
        return _typer_module

    try:
        module = importlib.import_module("typer")
    except ModuleNotFoundError as exc:  # noqa: PERF203
        msg = "The `typer` dependency is unavailable. Install the `bioetl[cli]` extra."
        raise RuntimeError(msg) from exc

    _typer_module = cast(TyperModule, module)
    return _typer_module

