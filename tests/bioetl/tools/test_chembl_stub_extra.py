"""Дополнительные тесты для chembl_stub."""

from __future__ import annotations

from typing import Mapping

import pytest

from bioetl.tools import chembl_stub as module


def test_offline_query_only_keeps_requested_field() -> None:
    query = module._OfflineQuery([{"a": 1, "b": 2}])

    result = list(query.only("a"))

    assert result == [{"a": 1}]


def test_offline_resource_filter_respects_limit_and_offset() -> None:
    resource = module._OfflineResource(
        [
            {"row": 1},
            {"row": 2},
            {"row": 3},
        ]
    )

    query = resource.filter(limit=1, offset=1)

    assert list(query) == [{"row": 2}]


@pytest.mark.parametrize(
    "value,expected",
    [
        (None, 7),
        (True, 1),
        (False, 0),
        (10, 10),
        (9.2, 9),
        ("12", 12),
        ("bad", 7),
    ],
)
def test_safe_int(value: object, expected: int) -> None:
    assert module._safe_int(value, 7) == expected


def test_get_offline_new_client_returns_resources() -> None:
    client = module.get_offline_new_client()

    assert isinstance(client.activity._records, list)
    assert list(client.activity.filter(limit=1))  # не пустой результат

