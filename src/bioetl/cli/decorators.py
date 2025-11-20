"""Общие декораторы и опции для CLI."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

import typer


def option_config(help_text: str | None = None) -> Any:
    """Опция Typer для пути к конфигурационному файлу."""

    return typer.Option(
        None,
        "--config",
        "-c",
        help=help_text or "Путь к конфигурационному файлу.",
        exists=True,
        dir_okay=False,
        file_okay=True,
        readable=True,
        resolve_path=True,
        show_default=False,
    )


def common_options(func: Callable[..., Any]) -> Callable[..., Any]:
    """Декоратор, добавляющий стандартные опции CLI."""

    def wrapper(
        config: Path | None = option_config(),
        verbose: bool = typer.Option(False, "--verbose", "-v", help="Подробный вывод."),
    ) -> Any:
        return func(config=config, verbose=verbose)

    wrapper.__name__ = getattr(func, "__name__", wrapper.__name__)
    wrapper.__doc__ = getattr(func, "__doc__", wrapper.__doc__)
    wrapper.__qualname__ = getattr(func, "__qualname__", wrapper.__qualname__)
    
    return wrapper


__all__ = ["option_config", "common_options"]
