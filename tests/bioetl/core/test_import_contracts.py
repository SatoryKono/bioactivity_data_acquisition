"""Smoke-тесты, гарантирующие отсутствие циклов импорта конфигурации и пайплайнов."""

from __future__ import annotations

import importlib
import sys
from collections.abc import Iterable

import pytest


def _purge_modules(prefixes: Iterable[str]) -> None:
    """Удалить указанные модули и их подмодули из sys.modules."""

    for prefix in prefixes:
        to_delete = [
            name
            for name in sys.modules
            if name == prefix or name.startswith(f"{prefix}.")
        ]
        for name in to_delete:
            sys.modules.pop(name, None)


@pytest.mark.parametrize(
    "import_order",
    (
        ("bioetl.config", "bioetl.pipelines.base"),
        ("bioetl.pipelines.base", "bioetl.config"),
    ),
)
def test_config_and_pipeline_import_orders(import_order: tuple[str, str]) -> None:
    """Импорт в любом порядке не должен вызывать RecursionError."""

    _purge_modules({"bioetl.config", "bioetl.pipelines"})
    for module_name in import_order:
        importlib.import_module(module_name)


def test_config_contracts_import_exposes_protocols() -> None:
    """Прямой импорт нового модуля доступен независимо от порядка загрузки."""

    _purge_modules({"bioetl.core.config_contracts"})
    module = importlib.import_module("bioetl.core.config_contracts")
    assert hasattr(module, "PipelineConfigProtocol")

