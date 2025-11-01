"""Tests for the ChEMBL activity client."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pytest

from bioetl.sources.chembl.activity.client.activity_client import ActivityChEMBLClient


class DummyParser:
    """Return activities as-is for easier assertions in tests."""

    def __init__(self) -> None:
        self.release: str | None = None

    def set_chembl_release(self, release: str | None) -> None:
        self.release = release

    def parse(self, activity: dict[str, Any]) -> dict[str, Any]:
        return dict(activity)


class DummyPipeline:
    """Minimal pipeline stub exposing the hooks used by the client under test."""

    def __init__(self, api_client: Mock) -> None:
        self._chembl_context = SimpleNamespace(
            client=api_client,
            batch_size=25,
            max_url_length=None,
            base_url="https://www.ebi.ac.uk/chembl/api/data",
        )
        self.registered_client: Mock | None = None

    def _init_chembl_client(self, *, defaults: dict[str, Any] | None = None, batch_size_cap: int | None = None):
        return self._chembl_context

    def register_client(self, client: Mock) -> None:
        self.registered_client = client


@pytest.mark.parametrize("batch_size", [25, 50])
def test_fetch_batch_passes_limit_parameter(batch_size: int) -> None:
    """The client should request enough rows for the batched identifiers."""

    activities = [{"activity_id": index} for index in range(1, batch_size + 1)]

    api_client = Mock()
    api_client.request_json.return_value = {"activities": activities}
    api_client.config = SimpleNamespace(base_url="https://www.ebi.ac.uk/chembl/api/data")

    pipeline = DummyPipeline(api_client)
    parser = DummyParser()
    client = ActivityChEMBLClient(pipeline, parser=parser)
    client.set_fallback_factory(
        lambda activity_id, reason, error: {
            "activity_id": activity_id,
            "source_system": "ChEMBL_FALLBACK",
        }
    )

    batch_ids = list(range(1, batch_size + 1))
    client._fetch_batch(batch_ids)

    expected_ids = ",".join(map(str, batch_ids))
    api_client.request_json.assert_called_once_with(
        "/activity.json",
        params={"activity_id__in": expected_ids, "limit": min(batch_size, 1000)},
    )
