from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

import pytest

# Избегаем загрузки корневого ``bioetl.__init__`` с тяжёлыми зависимостями.
import importlib.util
import sys
import types

PROJECT_ROOT = Path(__file__).resolve().parents[2]
src_root = PROJECT_ROOT / "src" / "bioetl"

bioetl_pkg = sys.modules.setdefault("bioetl", types.ModuleType("bioetl"))
bioetl_pkg.__path__ = [str(src_root)]

clients_pkg = sys.modules.setdefault(
    "bioetl.clients", types.ModuleType("bioetl.clients")
)
clients_pkg.__path__ = [str(src_root / "clients")]

core_pkg = sys.modules.setdefault("bioetl.core", types.ModuleType("bioetl.core"))
core_pkg.__path__ = [str(src_root / "core")]

chembl_spec = importlib.util.spec_from_file_location(
    "bioetl.clients.chembl", src_root / "clients" / "chembl.py"
)
chembl_module = importlib.util.module_from_spec(chembl_spec)
sys.modules["bioetl.clients.chembl"] = chembl_module
if chembl_spec and chembl_spec.loader:
    chembl_spec.loader.exec_module(chembl_module)

from bioetl.clients.base.client import (
    ClientRequest,
    Page,
    PaginationParams,
    RequestContext,
)
from bioetl.clients.chembl import ChemblClient
from bioetl.clients.crossref import CrossrefWorkClient
from bioetl.clients.openalex import OpenalexWorkClient
from bioetl.clients.pubchem import PubchemCompoundClient
from bioetl.clients.pubmed import PubmedArticleClient
from bioetl.clients.semantic_scholar import SemanticScholarPaperClient
from bioetl.clients.uniprot import UniprotProteinClient
from bioetl.core.http.transport import HttpTransport


class FakeHttpTransport(HttpTransport):
    """Тестовый транспорт, сохраняющий последние вызовы."""

    def __init__(
        self,
        *,
        fetch_one_result: Mapping[str, Any] | None = None,
        records: Iterable[Mapping[str, Any]] | None = None,
        pages: Iterable[Page] | None = None,
    ) -> None:
        self.fetch_one_result = fetch_one_result
        self.records = list(records or [])
        self.pages = list(pages or [])
        self.last_endpoint: str | None = None
        self.last_request: Any = None
        self.last_context: RequestContext | None = None

    def fetch_one(
        self,
        *,
        endpoint: str,
        params: Mapping[str, Any] | None = None,
        pagination: Any | None = None,
        raw: Any | None = None,
        context: RequestContext | None = None,
        request: Any | None = None,
    ) -> Any:
        self.last_endpoint = endpoint
        self.last_context = context
        self.last_request = request if request is not None else {
            "params": params,
            "pagination": pagination,
            "raw": raw,
        }
        return self.fetch_one_result

    def iter_records(
        self,
        *,
        endpoint: str,
        params: Mapping[str, Any] | None = None,
        pagination: Any | None = None,
        raw: Any | None = None,
        context: RequestContext | None = None,
        request: Any | None = None,
    ) -> Iterable[Mapping[str, Any]]:
        self.last_endpoint = endpoint
        self.last_context = context
        self.last_request = request if request is not None else {
            "params": params,
            "pagination": pagination,
            "raw": raw,
        }
        return iter(self.records)

    def iter_pages(
        self,
        *,
        endpoint: str,
        params: Mapping[str, Any] | None = None,
        pagination: Any | None = None,
        raw: Any | None = None,
        context: RequestContext | None = None,
        request: Any | None = None,
    ) -> Iterable[Page]:
        self.last_endpoint = endpoint
        self.last_context = context
        self.last_request = request if request is not None else {
            "params": params,
            "pagination": pagination,
            "raw": raw,
        }
        return iter(self.pages)

    def close(self) -> None:  # pragma: no cover - интерфейс совместимости
        return None


@dataclass(slots=True)
class _ClientCase:
    client_cls: type
    endpoint: str
    id_field: str
    filter_key: str
    name: str


CLIENT_CASES = (
    _ClientCase(
        client_cls=PubchemCompoundClient,
        endpoint="/compound",
        id_field="cid",
        filter_key="pub_status",
        name="pubchem.compound",
    ),
    _ClientCase(
        client_cls=PubmedArticleClient,
        endpoint="/article",
        id_field="pmid",
        filter_key="term",
        name="pubmed.article",
    ),
    _ClientCase(
        client_cls=OpenalexWorkClient,
        endpoint="/works",
        id_field="ids",
        filter_key="filter[concepts.id]",
        name="openalex.work",
    ),
    _ClientCase(
        client_cls=CrossrefWorkClient,
        endpoint="/works",
        id_field="doi",
        filter_key="filter[author]",
        name="crossref.work",
    ),
    _ClientCase(
        client_cls=SemanticScholarPaperClient,
        endpoint="/paper",
        id_field="paperId",
        filter_key="fieldsOfStudy",
        name="semantic_scholar.paper",
    ),
    _ClientCase(
        client_cls=UniprotProteinClient,
        endpoint="/proteins",
        id_field="accessions",
        filter_key="organism",
        name="uniprot.protein",
    ),
)


@pytest.mark.parametrize("case", CLIENT_CASES)
def test_http_client_maps_request_and_delegates(case: _ClientCase) -> None:
    transport = FakeHttpTransport(
        fetch_one_result={"kind": "one"},
        records=[{"kind": "record"}],
        pages=[Page(items=[{"kind": "page"}], next_offset=None, has_next=False)],
    )
    client = case.client_cls(
        name=case.name,
        transport=transport,
    )
    pagination = PaginationParams(limit=7, offset=3, page_size=2, max_pages=5)
    request = ClientRequest(
        ids=["ID-1"],
        filters={"status": "active", "raw": "keep"},
        pagination=pagination,
        raw={"q": "raw"},
    )
    context = RequestContext(trace_id="ctx-1", timeout_s=0.1, max_retries=1)

    assert client.fetch_one(request, context=context) == {"kind": "one"}
    assert transport.last_endpoint == case.endpoint
    assert transport.last_context is context
    assert transport.last_request["params"] == {
        case.filter_key: "active",
        "raw": "keep",
        case.id_field: ["ID-1"],
    }
    assert transport.last_request["pagination"] is pagination
    assert transport.last_request["raw"] == {"q": "raw"}

    assert list(client.iter_records(request, context=context)) == [
        {"kind": "record"}
    ]
    assert transport.last_endpoint == case.endpoint
    assert transport.last_request["params"][case.id_field] == ["ID-1"]
    assert transport.last_request["pagination"] is pagination

    pages = list(client.iter_pages(request, context=context))
    assert pages == [Page(items=[{"kind": "page"}], next_offset=None, has_next=False)]
    assert transport.last_endpoint == case.endpoint
    assert transport.last_request["pagination"] is pagination


def test_chembl_client_maps_ids_filters_and_pagination() -> None:
    transport = FakeHttpTransport(
        fetch_one_result={"resource": "chembl"},
        records=[{"resource": "chembl"}],
        pages=[Page(items=[{"resource": "chembl"}], next_offset=1, has_next=True)],
    )
    client = ChemblClient(
        name="chembl.target",
        transport=transport,
    )
    pagination = PaginationParams(limit=5, offset=2, page_size=10, max_pages=3)
    request = ClientRequest(
        ids=["CHEMBL1", "CHEMBL2"],
        filters={"pref_name": "EGFR", "organism": "human"},
        pagination=pagination,
        raw={"extra": True},
    )
    context = RequestContext(trace_id="chembl")

    assert client.fetch_one(request, context=context) == {"resource": "chembl"}
    assert transport.last_endpoint == "/target"
    assert transport.last_context is context
    mapped_request = transport.last_request
    assert isinstance(mapped_request, ClientRequest)
    assert mapped_request.filters == {
        "pref_name__iexact": "EGFR",
        "organism": "human",
        "target_chembl_id": "CHEMBL1",
    }
    assert mapped_request.pagination is pagination
    assert mapped_request.raw == {"extra": True}

    assert list(client.iter_records(request, context=context)) == [
        {"resource": "chembl"}
    ]
    mapped_request = transport.last_request
    assert isinstance(mapped_request, ClientRequest)
    assert mapped_request.filters["target_chembl_id"] == ["CHEMBL1", "CHEMBL2"]

    pages = list(client.iter_pages(request, context=context))
    assert pages == [Page(items=[{"resource": "chembl"}], next_offset=1, has_next=True)]
    assert transport.last_request.pagination is pagination
