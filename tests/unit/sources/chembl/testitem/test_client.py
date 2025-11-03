from __future__ import annotations

from types import SimpleNamespace

import pytest
from requests.exceptions import HTTPError

from bioetl.sources.chembl.testitem.client import TestItemChEMBLClient
from bioetl.sources.chembl.testitem.parser import TestItemParser
from bioetl.sources.chembl.testitem.pipeline import TestItemPipeline
from bioetl.sources.chembl.testitem.request import TestItemRequestBuilder
from bioetl.utils.fallback import FallbackRecordBuilder


class StubAPIClient:
    def __init__(self) -> None:
        self.config = SimpleNamespace(base_url="https://api.example")
        self.calls: list[tuple[str, tuple[tuple[str, str], ...] | None]] = []
        self.responses: dict[tuple[str, tuple[tuple[str, str], ...] | None], object] = {}

    def request_json(self, url: str, params: dict[str, str] | None = None) -> dict[str, object]:
        key = (url, tuple(sorted((params or {}).items())))
        self.calls.append(key)
        response = self.responses.get(key)
        if isinstance(response, Exception):
            raise response
        if response is None:
            raise AssertionError(f"Unexpected request: {key}")
        return response  # type: ignore[return-value]


@pytest.fixture()
def parser() -> TestItemParser:
    return TestItemParser(
        expected_columns=TestItemPipeline._expected_columns(),
        property_fields=TestItemPipeline._CHEMBL_PROPERTY_FIELDS,
        structure_fields=TestItemPipeline._CHEMBL_STRUCTURE_FIELDS,
        json_fields=TestItemPipeline._CHEMBL_JSON_FIELDS,
        text_fields=TestItemPipeline._CHEMBL_TEXT_FIELDS,
        fallback_fields=TestItemPipeline._FALLBACK_FIELDS,
    )


@pytest.fixture()
def fallback_builder() -> FallbackRecordBuilder:
    return FallbackRecordBuilder(
        business_columns=TestItemPipeline._expected_columns(),
        context={"chembl_release": "test"},
    )


def build_client(
    api_client: StubAPIClient,
    parser: TestItemParser,
    fallback_builder: FallbackRecordBuilder,
) -> TestItemChEMBLClient:
    request_builder = TestItemRequestBuilder(
        api_client=api_client,
        batch_size=50,
        max_url_length=None,
    )

    return TestItemChEMBLClient(
        api_client=api_client,
        batch_size=50,
        chembl_release="test",
        molecule_cache={},
        request_builder=request_builder,
        parser=parser,
        fallback_builder=fallback_builder,
    )


def test_fetch_molecules_uses_cache(parser: TestItemParser, fallback_builder: FallbackRecordBuilder) -> None:
    api_client = StubAPIClient()
    params_key = ("/molecule.json", tuple(sorted({"molecule_chembl_id__in": "CHEMBL1", "limit": "1"}.items())))
    api_client.responses[params_key] = {
        "molecules": [
            {
                "molecule_chembl_id": "CHEMBL1",
                "molregno": 1,
                "pref_name": "Sample",
            }
        ]
    }

    client = build_client(api_client, parser, fallback_builder)

    records, stats = client.fetch_molecules(["CHEMBL1"])
    assert len(records) == 1
    assert stats["api_success_count"] == 1
    assert stats["cache_hits"] == 0

    # second call should reuse cache
    records_again, stats_again = client.fetch_molecules(["CHEMBL1"])
    assert len(records_again) == 1
    assert stats_again["cache_hits"] == 1
    assert len(api_client.calls) == 1


def test_fetch_molecules_handles_missing_with_fallback(
    parser: TestItemParser, fallback_builder: FallbackRecordBuilder
) -> None:
    api_client = StubAPIClient()
    params_key = ("/molecule.json", tuple(sorted({"molecule_chembl_id__in": "CHEMBL2", "limit": "1"}.items())))
    api_client.responses[params_key] = {"molecules": []}
    single_key = ("/molecule/CHEMBL2.json", tuple())
    api_client.responses[single_key] = HTTPError("not found")

    client = build_client(api_client, parser, fallback_builder)

    records, stats = client.fetch_molecules(["CHEMBL2"])

    assert len(records) == 1
    record = records[0]
    assert record["molecule_chembl_id"] == "CHEMBL2"
    assert record["fallback_error_code"] == "http_error"
    assert stats["fallback_count"] == 1
