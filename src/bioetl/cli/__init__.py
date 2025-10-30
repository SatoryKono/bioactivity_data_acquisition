"""CLI interface using Typer with lazy loading to avoid circular imports."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - import cycle guard for type checkers
    from typer import Typer

    app: Typer


def __getattr__(name: str) -> Any:
    if name == "app":
        from .main import app

        return app
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ["app"]
