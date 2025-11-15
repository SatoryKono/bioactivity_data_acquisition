"""Typer helpers for standalone BioETL devtool scripts."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol

import typer

__all__ = [
    "TyperApp",
    "get_typer",
    "register_tool_app",
    "run_app",
]


class TyperApp(Protocol):
    """Typer application interface exposed to devtool scripts."""

    def __call__(self, *_args: Any, **_kwargs: Any) -> None: ...

    def command(self, *_args: Any, **_kwargs: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]: ...

    def callback(self, *_args: Any, **_kwargs: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]: ...


def get_typer() -> Any:
    """Return the Typer module reference."""

    return typer


def _create_simple_tool_app(
    *,
    name: str,
    help_text: str,
    main_fn: Callable[..., Any],
) -> TyperApp:
    app = typer.Typer(
        name=name,
        help=help_text,
        add_completion=False,
        invoke_without_command=True,
    )
    app.callback()(main_fn)
    return app


def run_app(app: TyperApp) -> None:
    """Execute a Typer application."""

    app()


def register_tool_app(
    *,
    name: str,
    help_text: str,
    main_fn: Callable[..., Any],
) -> tuple[TyperApp, Callable[[], None]]:
    """Return a configured Typer app and execution wrapper for simple tools."""

    app = _create_simple_tool_app(name=name, help_text=help_text, main_fn=main_fn)

    def _run() -> None:
        run_app(app)

    return app, _run


