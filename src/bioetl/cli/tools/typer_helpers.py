"""Helper types and factories for Typer-based CLI applications."""

from __future__ import annotations

import importlib
from collections.abc import Callable
from typing import Any, Protocol, TypeVar, cast

__all__ = [
    "TyperApp",
    "TyperModule",
    "create_app",
    "create_simple_tool_app",
    "get_typer",
    "run_app",
]

_F = TypeVar("_F", bound=Callable[..., Any])
_typer_module: TyperModule | None = None


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
    Option: Callable[..., Any]
    echo: Callable[..., None]
    secho: Callable[..., None]
    colors: Any


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


def get_typer() -> Any:
    """Return the cached ``typer`` module as a dynamically typed reference."""

    return cast(Any, _load_typer())


def create_app(name: str, help_text: str) -> TyperApp:
    """Create a Typer application with completion disabled."""

    typer = _load_typer()
    return typer.Typer(name=name, help=help_text, add_completion=False)


def create_simple_tool_app(
    *,
    name: str,
    help_text: str,
    main_fn: Callable[..., Any],
) -> TyperApp:
    """Create an application and register a single ``main`` command."""

    app = create_app(name=name, help_text=help_text)
    app.command()(main_fn)
    return app


def run_app(app: TyperApp) -> None:
    """Invoke a Typer application entrypoint."""

    app()

