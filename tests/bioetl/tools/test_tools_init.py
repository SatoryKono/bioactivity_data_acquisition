from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest
import typer

from bioetl.tools import get_project_root, load_typer_app


def test_get_project_root_matches_module_root() -> None:
    import bioetl.tools as tools_module

    expected = Path(tools_module.__file__).resolve().parents[3]
    assert get_project_root() == expected


def test_load_typer_app_returns_module_app(monkeypatch: pytest.MonkeyPatch) -> None:
    module_name = "tests.fake_typer_module"
    fake_module = types.ModuleType(module_name)
    fake_app = typer.Typer()
    fake_module.app = fake_app
    monkeypatch.setitem(sys.modules, module_name, fake_module)
    try:
        loaded = load_typer_app(module_name)
        assert loaded is fake_app
    finally:
        monkeypatch.delitem(sys.modules, module_name, raising=False)


def test_load_typer_app_missing_attribute(monkeypatch: pytest.MonkeyPatch) -> None:
    module_name = "tests.fake_module_without_app"
    fake_module = types.ModuleType(module_name)
    monkeypatch.setitem(sys.modules, module_name, fake_module)
    try:
        with pytest.raises(RuntimeError):
            load_typer_app(module_name)
    finally:
        monkeypatch.delitem(sys.modules, module_name, raising=False)


def test_load_typer_app_invalid_type(monkeypatch: pytest.MonkeyPatch) -> None:
    module_name = "tests.fake_module_wrong_type"
    fake_module = types.ModuleType(module_name)
    fake_module.app = object()
    monkeypatch.setitem(sys.modules, module_name, fake_module)
    try:
        with pytest.raises(TypeError):
            load_typer_app(module_name)
    finally:
        monkeypatch.delitem(sys.modules, module_name, raising=False)


