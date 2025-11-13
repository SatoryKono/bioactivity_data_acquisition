"""Тесты базовых утилит из `bioetl.tools`."""

from __future__ import annotations

import sys
from types import ModuleType
from pathlib import Path

import pytest
import typer

from bioetl.tools import get_project_root, load_typer_app


@pytest.mark.unit
def test_get_project_root_monkeypatched(monkeypatch, tmp_path: Path) -> None:
    """Проверяем вычисление корня проекта без обращения к реальной FS."""

    fake_repo = tmp_path / "repo"
    fake_module_path = fake_repo / "src" / "bioetl" / "tools" / "dummy.py"

    def fake_resolve(_: Path) -> Path:
        return fake_module_path

    monkeypatch.setattr("bioetl.tools.Path.resolve", fake_resolve, raising=False)

    root = get_project_root()

    assert root == fake_repo


@pytest.mark.unit
def test_load_typer_app_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """Успешная загрузка Typer-приложения из временного модуля."""

    module_name = "tests.fake_tools.app_success"
    module = ModuleType(module_name)
    module.app = typer.Typer()
    monkeypatch.setitem(sys.modules, module_name, module)

    app = load_typer_app(module_name)

    assert app is module.app


@pytest.mark.unit
def test_load_typer_app_missing_attribute(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ошибка при отсутствии атрибута `app`."""

    module_name = "tests.fake_tools.app_missing"
    module = ModuleType(module_name)
    monkeypatch.setitem(sys.modules, module_name, module)

    with pytest.raises(RuntimeError, match="does not define Typer app 'app'"):
        load_typer_app(module_name)


@pytest.mark.unit
def test_load_typer_app_wrong_type(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ошибка при наличии атрибута, не являющегося `typer.Typer`."""

    module_name = "tests.fake_tools.app_wrong_type"
    module = ModuleType(module_name)
    module.app = object()
    monkeypatch.setitem(sys.modules, module_name, module)

    with pytest.raises(TypeError, match="is not a Typer application"):
        load_typer_app(module_name)

