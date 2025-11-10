"""Тесты для канонических модулей конфигураций."""

from __future__ import annotations

import importlib
import sys

import pytest


@pytest.mark.unit
@pytest.mark.parametrize(
    ("module_name", "attributes"),
    [
        ("bioetl.config.models.base", ("PipelineConfig", "PipelineMetadata")),
        ("bioetl.config.models.determinism", ("DeterminismConfig", "DeterminismSortingConfig")),
        ("bioetl.config.models.http", ("HTTPClientConfig", "HTTPConfig")),
    ],
)
def test_canonical_config_modules_expose_expected_symbols(
    module_name: str,
    attributes: tuple[str, ...],
) -> None:
    """Проверяет, что прямые модули содержат публичные классы."""
    module = importlib.import_module(module_name)

    for attr in attributes:
        assert hasattr(module, attr), f"Модуль {module_name} не содержит {attr}"


@pytest.mark.unit
def test_legacy_models_module_emits_deprecation_and_reexports() -> None:
    sys.modules.pop("bioetl.config.models", None)
    with pytest.warns(DeprecationWarning):
        legacy_module = importlib.import_module("bioetl.config.models")

    from bioetl.config.models.models import PipelineConfig, PipelineMetadata

    assert legacy_module.PipelineConfig is PipelineConfig
    assert legacy_module.PipelineMetadata is PipelineMetadata
