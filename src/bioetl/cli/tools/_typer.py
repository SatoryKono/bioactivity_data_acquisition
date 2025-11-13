"""Helper types and factories for Typer-based CLI applications."""

from __future__ import annotations

import importlib
from collections.abc import Callable
from typing import Any, Protocol, TypeVar, cast

__all__ = ["TyperApp", "TyperModule", "create_app", "run_app"]

_F = TypeVar("_F", bound=Callable[..., Any])


class TyperApp(Protocol):
    """Typer application interface exposed to local utilities."""

    def __call__(self, *args: Any, **kwargs: Any) -> None:
        """Invoke the Typer application entrypoint."""
        ...

    def command(self, *args: Any, **kwargs: Any) -> Callable[[_F], _F]:
        """Register a Typer command callback."""
        ...


class TyperModule(Protocol):
    """Minimal contract of the ``typer`` module required by this package."""

    Typer: Callable[..., TyperApp]


def _load_typer() -> TyperModule:
    """Import ``typer`` and raise a descriptive error when the dependency is missing."""

    try:
        module = importlib.import_module("typer")
    except ModuleNotFoundError as exc:  # noqa: PERF203
        msg = "The `typer` dependency is unavailable. Install the `bioetl[cli]` extra."
        raise RuntimeError(msg) from exc
    return cast(TyperModule, module)


def create_app(name: str, help_text: str) -> TyperApp:
    """Create a Typer application with completion disabled."""

    typer = _load_typer()
    return typer.Typer(name=name, help=help_text, add_completion=False)


def run_app(app: TyperApp) -> None:
    """Invoke a Typer application entrypoint."""

    app()

