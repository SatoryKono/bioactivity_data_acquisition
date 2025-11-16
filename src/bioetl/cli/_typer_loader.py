"""Utilities for loading the optional :mod:`typer` dependency."""

from __future__ import annotations

import importlib
import sys
from collections.abc import Callable
from types import ModuleType
from typing import Any, Protocol, cast


def _get_helpers_module() -> ModuleType | None:
    """Return the devtool typer_helpers module when already imported."""

    return sys.modules.get("bioetl.devtools.typer_helpers")

__all__ = ["TyperModule", "_load_typer"]


class TyperModule(Protocol):
    """Minimal contract of the ``typer`` module required by this package."""

    Typer: Callable[..., Any]
    Option: Callable[..., Any]
    echo: Callable[..., None]
    secho: Callable[..., None]
    colors: Any


_typer_module: TyperModule | None = None
_SENTINEL = object()


def _sync_helper_cache(value: TyperModule | None) -> None:
    """Keep ``bioetl.devtools.typer_helpers`` cache in sync when present."""

    helpers_module = _get_helpers_module()
    if helpers_module is None:
        return
    setattr(helpers_module, "_typer_module", value)


def _load_typer() -> TyperModule:
    """Import ``typer`` and raise a descriptive error when the dependency is missing."""

    global _typer_module
    helpers_module = _get_helpers_module()
    helper_cache = (
        getattr(helpers_module, "_typer_module", _SENTINEL) if helpers_module else _SENTINEL
    )
    if helper_cache is None:
        _typer_module = None
    elif helper_cache is not _SENTINEL and _typer_module is None:
        _typer_module = cast(TyperModule, helper_cache)

    if _typer_module is not None:
        return _typer_module

    try:
        module = importlib.import_module("typer")
    except ModuleNotFoundError as exc:  # noqa: PERF203
        msg = "The `typer` dependency is unavailable. Install the `bioetl[cli]` extra."
        raise RuntimeError(msg) from exc

    _typer_module = cast(TyperModule, module)
    _sync_helper_cache(_typer_module)
    return _typer_module

