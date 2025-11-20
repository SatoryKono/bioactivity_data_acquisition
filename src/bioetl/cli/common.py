"""Общие утилиты для построения Typer CLI."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Any, Callable

import typer

from .decorators import common_options as _common_options

TyperApp = typer.Typer
CommandSpec = Iterable[tuple[str, Callable[..., Any]]]


def create_app(name: str, version: str | None = None) -> TyperApp:
    """Создать Typer-приложение с типовыми настройками."""

    app = typer.Typer(
        name=name,
        add_completion=False,
        no_args_is_help=True,
        add_help_option=True,
        context_settings={"help_option_names": ["-h", "--help"]},
    )
    if version is not None:
        setattr(app, "__version__", version)
    return app


def register_commands(app: TyperApp, commands: CommandSpec) -> TyperApp:
    """Зарегистрировать набор команд в приложении Typer."""

    for name, command in commands:
        app.command(name=name)(command)
    return app


def run_app(app: TyperApp, argv: list[str] | None = None) -> int:
    """Запустить приложение Typer, логируя ошибки и возвращая exit code."""

    logger = logging.getLogger(__name__)
    try:
        result = app(args=argv, standalone_mode=False)
        return 0 if result is None else int(result)
    except typer.Exit as exc:  # pragma: no cover - штатные завершения
        logger.debug("CLI requested exit", extra={"exit_code": exc.exit_code})
        return int(getattr(exc, "exit_code", getattr(exc, "code", 0)) or 0)
    except Exception:  # noqa: BLE001
        logger.exception("CLI execution failed")
        return 1


def common_options(func: Callable[..., Any]) -> Callable[..., Any]:
    """Прокси к общему декоратору опций для удобного импорта."""

    return _common_options(func)


__all__ = [
    "TyperApp",
    "create_app",
    "register_commands",
    "run_app",
    "common_options",
]
