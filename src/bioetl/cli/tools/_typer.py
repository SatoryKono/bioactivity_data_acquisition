"""Вспомогательные типы и фабрики для CLI-приложений Typer."""

from __future__ import annotations

import importlib
from collections.abc import Callable
from typing import Any, Protocol, TypeVar, cast

__all__ = ["TyperApp", "TyperModule", "create_app", "run_app"]

_F = TypeVar("_F", bound=Callable[..., Any])


class TyperApp(Protocol):
    """Интерфейс приложения Typer, необходимый для локальных утилит."""

    def __call__(self, *args: Any, **kwargs: Any) -> None:
        ...

    def command(self, *args: Any, **kwargs: Any) -> Callable[[_F], _F]:
        ...


class TyperModule(Protocol):
    """Минимальный контракт модуля `typer`, требуемый в этом пакете."""

    Typer: Callable[..., TyperApp]


def _load_typer() -> TyperModule:
    """Возвращает модуль `typer`, проверяя наличие зависимости."""

    try:
        module = importlib.import_module("typer")
    except ModuleNotFoundError as exc:  # noqa: PERF203
        msg = "Зависимость `typer` недоступна. Установите extras `bioetl[cli]`."
        raise RuntimeError(msg) from exc
    return cast(TyperModule, module)


def create_app(name: str, help_text: str) -> TyperApp:
    """Создаёт Typer-приложение без автодополнения."""

    typer = _load_typer()
    return typer.Typer(name=name, help=help_text, add_completion=False)


def run_app(app: TyperApp) -> None:
    """Единая точка входа для CLI-утилит."""

    app()

