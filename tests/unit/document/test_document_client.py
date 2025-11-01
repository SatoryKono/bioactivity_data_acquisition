"""Unit tests for :mod:`bioetl.sources.chembl.document.client`."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
import requests

from bioetl.config.loader import load_config
from bioetl.core.chembl.client import ChemblClientContext
from bioetl.sources.chembl.document.client import (
    DocumentChEMBLClient,
    DocumentFetchCallbacks,
)


class StubAPIClient:
    """Simple API client stub recording calls made during tests."""

    def __init__(self, responses: list[object]):
        self.responses = responses
        self.calls: list[tuple[str, dict[str, str]]] = []
        self.config = SimpleNamespace(base_url="https://example.org")

    def request_json(self, endpoint: str, params: dict[str, str]):
        self.calls.append((endpoint, params))
        if not self.responses:
            return {"documents": []}
        result = self.responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


@pytest.fixture
def stub_context(monkeypatch) -> ChemblClientContext:
    """Provide a patched ChemBL context returning predictable responses."""

    responses: list[object] = [
        {"documents": [{"document_chembl_id": "CHEMBL1"}, {"document_chembl_id": "CHEMBL2"}]}
    ]
    client = StubAPIClient(responses)
    context = ChemblClientContext(
        client=client,
        source_config=SimpleNamespace(),
        batch_size=2,
        max_url_length=500,
        base_url="https://example.org",
    )

    monkeypatch.setattr(
        "bioetl.sources.chembl.document.client.document_client.build_chembl_client_context",
        lambda config, defaults=None, batch_size_cap=None: context,
    )

    dummy_config = load_config("configs/pipelines/document.yaml")
    return context, dummy_config


def _callbacks():
    return DocumentFetchCallbacks(
        classify_error=lambda exc: "ERROR",
        create_fallback=lambda document_id, error_type, message, error: {
            "document_chembl_id": document_id,
            "fallback_error_code": error_type,
        },
    )


def test_fetch_documents_uses_cache_for_subsequent_calls(stub_context) -> None:
    context, pipeline_config = stub_context
    client = DocumentChEMBLClient(pipeline_config)
    callbacks = _callbacks()

    first_call = client.fetch_documents(["CHEMBL1", "CHEMBL2"], callbacks)
    second_call = client.fetch_documents(["CHEMBL1"], callbacks)

    assert len(first_call) == 2
    assert len(second_call) == 1
    assert context.client.calls[0][1]["document_chembl_id__in"].split(",") == ["CHEMBL1", "CHEMBL2"]
    # Second call should be served entirely from cache
    assert len(context.client.calls) == 1


def test_fetch_documents_splits_on_url_length(stub_context) -> None:
    context, pipeline_config = stub_context
    # Replace the underlying responses to return deterministic payloads for two calls
    context.client.responses = [
        {"documents": [{"document_chembl_id": "CHEMBL1"}]},
        {"documents": [{"document_chembl_id": "CHEMBL2"}]},
    ]
    context.max_url_length = 5
    client = DocumentChEMBLClient(pipeline_config)
    client.release = "TEST"
    callbacks = _callbacks()

    records = client.fetch_documents(["CHEMBL1", "CHEMBL2"], callbacks)

    assert [record["document_chembl_id"] for record in records] == ["CHEMBL1", "CHEMBL2"]
    assert len(context.client.calls) == 2


def test_fetch_documents_creates_fallback_on_error(stub_context) -> None:
    context, pipeline_config = stub_context
    context.client.responses = [requests.exceptions.ReadTimeout("boom")]
    context.batch_size = 1
    client = DocumentChEMBLClient(pipeline_config)
    client.release = "TEST"
    callbacks = _callbacks()

    records = client.fetch_documents(["CHEMBL1"], callbacks)

    assert records == [
        {
            "document_chembl_id": "CHEMBL1",
            "fallback_error_code": "ERROR",
        }
    ]
