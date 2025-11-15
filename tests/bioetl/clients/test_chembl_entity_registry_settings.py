"""Тесты для функций разрешения настроек ChEMBL-реестра."""

from __future__ import annotations

import pytest

from bioetl.clients.chembl_entity_registry import (
    _resolve_batch_size,
    _resolve_max_url_length,
)


@pytest.mark.unit
def test_resolve_batch_size_rejects_negative_value() -> None:
    """Отрицательный batch_size приводит к ошибке."""

    with pytest.raises(ValueError, match="batch_size должен быть ≥ 1"):
        _resolve_batch_size(None, {"batch_size": -5})


@pytest.mark.unit
def test_resolve_max_url_length_allows_none() -> None:
    """None допустим для max_url_length, когда это разрешено."""

    assert _resolve_max_url_length(None, {"max_url_length": None}) is None


@pytest.mark.unit
def test_resolve_max_url_length_rejects_non_positive() -> None:
    """Нулевое значение max_url_length отклоняется."""

    with pytest.raises(ValueError, match="max_url_length должен быть ≥ 1"):
        _resolve_max_url_length(None, {"max_url_length": 0})

