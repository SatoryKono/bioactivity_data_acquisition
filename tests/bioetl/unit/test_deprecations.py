"""Regression tests for deprecation warnings exposed by public modules."""

from __future__ import annotations

import importlib
import warnings

import pytest


@pytest.mark.parametrize("attr_name", ["BaseApiClient", "IParser", "INormalizer"])
def test_core_exports_raise_deprecation_warning(attr_name: str) -> None:
    """Importing moved symbols from ``bioetl.core`` should emit a warning."""

    core_module = importlib.reload(importlib.import_module("bioetl.core"))
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", DeprecationWarning)
        attr = getattr(core_module, attr_name)

    new_module = importlib.import_module("bioetl.base_classes")
    expected = getattr(new_module, attr_name)

    assert attr is expected, "deprecated alias must resolve to the canonical symbol"
    assert any(issubclass(item.category, DeprecationWarning) for item in caught), (
        "accessing deprecated export should emit DeprecationWarning",
    )
