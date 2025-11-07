"""Вспомогательные утилиты BioETL для CLI-команд."""

from __future__ import annotations

from importlib import import_module
from pathlib import Path
from typing import Protocol

import typer

__all__ = [
    "get_project_root",
    "ToolRunnable",
    "load_typer_app",
]


def get_project_root() -> Path:
    """Возвращает корень репозитория."""

    return Path(__file__).resolve().parents[3]


class ToolRunnable(Protocol):
    """Контракт для функций, исполняющих бизнес-логику CLI-утилиты."""

    def __call__(self, *args: object, **kwargs: object) -> object:
        ...


def load_typer_app(module_path: str, app_name: str = "app") -> typer.Typer:
    """Динамически загружает Typer-приложение из модуля."""

    module = import_module(module_path)
    app = getattr(module, app_name, None)
    if app is None:
        raise RuntimeError(f"Module {module_path} does not define Typer app '{app_name}'")
    return app
