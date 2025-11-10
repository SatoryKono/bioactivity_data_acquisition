"""Дополнительные тесты для chembl_stub."""

from __future__ import annotations

import pytest

from bioetl.tools import chembl_stub as module


def test_offline_query_only_keeps_requested_field() -> None:
    query = module.OfflineQuery([{"a": 1, "b": 2}])

    result = list(query.only("a"))

    assert result == [{"a": 1}]


def test_offline_resource_filter_respects_limit_and_offset() -> None:
    resource = module.OfflineResource(
        [
            {"row": 1},
            {"row": 2},
            {"row": 3},
        ]
    )

    query = resource.filter(limit=1, offset=1)

    assert list(query) == [{"row": 2}]


@pytest.mark.parametrize(
    "limit,expected_length",
    [
        (None, 5),
        (True, 1),
        (False, 0),
        (10, 5),
        (9.2, 5),
        ("12", 5),
        ("bad", 5),
    ],
)
def test_offline_resource_coerces_limit(limit: object, expected_length: int) -> None:
    resource = module.OfflineResource([{"row": idx} for idx in range(5)])

    rows = list(resource.filter(limit=limit))

    assert len(rows) == expected_length


def test_get_offline_new_client_returns_resources() -> None:
    client = module.get_offline_new_client()

    first_page = client.activity.filter(limit=1)

    assert isinstance(first_page, module.OfflineQuery)
    assert list(first_page)  # не пустой результат

