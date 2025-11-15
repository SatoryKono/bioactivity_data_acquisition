"""Unified Typer helpers and entrypoints for BioETL CLI."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol, TypeVar, cast

from ._typer_loader import TyperModule, _load_typer

__all__ = [
    "TyperApp",
    "TyperModule",
    "create_app",
    "create_simple_tool_app",
    "get_typer",
    "register_tool_app",
    "run_app",
]


_F = TypeVar("_F", bound=Callable[..., Any])


class TyperApp(Protocol):
    """Typer application interface exposed to local utilities."""

    def __call__(self, *_args: Any, **_kwargs: Any) -> None:
        """Invoke the Typer application entrypoint."""
        ...

    def command(self, *_args: Any, **_kwargs: Any) -> Callable[[_F], _F]:
        """Register a Typer command callback."""
        ...

    def callback(self, *_args: Any, **_kwargs: Any) -> Callable[[_F], _F]:
        """Register a Typer application callback."""
        ...


def _noop_callback() -> None:
    """Placeholder group callback to force Typer into multi-command mode."""

    return None


def get_typer() -> TyperModule:
    """Return the cached Typer module instance or load it on demand."""

    return _load_typer()


def create_app(name: str, help_text: str) -> TyperApp:
    """Create a Typer application with completion disabled."""

    typer: TyperModule = get_typer()
    app = typer.Typer(
        name=name,
        help=help_text,
        add_completion=False,
        callback=_noop_callback,
    )
    return cast(TyperApp, app)


def run_app(app: TyperApp) -> None:
    """Invoke a Typer application entrypoint."""

    app()


def create_simple_tool_app(
    *,
    name: str,
    help_text: str,
    main_fn: Callable[..., None],
) -> tuple[TyperApp, Callable[[], None]]:
    """Create a Typer app, register ``main_fn`` as default command, and return runner."""

    app = create_app(name, help_text)
    app.command()(main_fn)
    return app, lambda: run_app(app)


def register_tool_app(
    *,
    name: str,
    help_text: str,
    main_fn: Callable[..., None],
) -> tuple[TyperApp, Callable[[], None]]:
    """Compatibility wrapper retained for historical modules."""

    return create_simple_tool_app(name=name, help_text=help_text, main_fn=main_fn)
