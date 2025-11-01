from __future__ import annotations

import http.server
import json
import socketserver
import threading
import types
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock
from urllib.parse import urlsplit

import pandas as pd
import pytest

from bioetl.config.loader import load_config
from bioetl.sources.uniprot import UniProtEnrichmentResult
from bioetl.sources.uniprot.pipeline import UniProtPipeline


@pytest.fixture()
def uniprot_config():
    return load_config("configs/pipelines/uniprot.yaml")


def test_transform_uses_service(monkeypatch: pytest.MonkeyPatch, uniprot_config) -> None:
    pipeline = UniProtPipeline(uniprot_config, "test-run")

    monkeypatch.setattr(
        "bioetl.sources.uniprot.pipeline.finalize_output_dataset",
        lambda df, **_: df.copy(),
    )

    enrichment_result = UniProtEnrichmentResult(
        dataframe=pd.DataFrame({"uniprot_accession": ["P12345"]}),
        silver=pd.DataFrame({"canonical_accession": ["P12345"]}),
        components=pd.DataFrame(),
        metrics={"enrichment_success.uniprot": 1.0},
        missing_mappings=[
            {
                "stage": "uniprot",
                "target_id": None,
                "accession": "P12345",
                "resolution": "direct",
                "status": "resolved",
            }
        ],
        validation_issues=[],
    )

    service_mock = MagicMock()
    service_mock.enrich_targets.return_value = enrichment_result
    pipeline.normalizer = service_mock

    df = pd.DataFrame({"uniprot_accession": ["P12345"]})
    transformed = pipeline.transform(df)

    service_mock.enrich_targets.assert_called_once()
    assert "uniprot_accession" in transformed.columns
    assert not pipeline.qc_missing_mappings.empty
    assert pipeline.qc_metrics["enrichment_success.uniprot"] == pytest.approx(1.0)
    assert "uniprot_entries" in pipeline.additional_tables


class _ThreadingHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True

    def __init__(self, server_address, RequestHandlerClass):  # type: ignore[override]
        super().__init__(server_address, RequestHandlerClass)
        self.routes: dict[tuple[str, str], tuple[int, dict[str, str], bytes]] = {}


class _RequestHandler(http.server.BaseHTTPRequestHandler):
    server: _ThreadingHTTPServer  # type: ignore[assignment]

    def log_message(self, format: str, *args: Any) -> None:  # pragma: no cover - silence stdlog
        return

    def do_GET(self) -> None:  # noqa: N802 - required by BaseHTTPRequestHandler
        self._handle("GET")

    def do_POST(self) -> None:  # noqa: N802 - required by BaseHTTPRequestHandler
        length = int(self.headers.get("Content-Length", "0"))
        if length:
            self.rfile.read(length)
        self._handle("POST")

    def _handle(self, method: str) -> None:
        path = urlsplit(self.path).path
        response = self.server.routes.get((method.upper(), path))
        if response is None:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"{}")
            return

        status, headers, body = response
        self.send_response(status)
        for key, value in headers.items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)


@dataclass
class _MiniHTTPServer:
    server: _ThreadingHTTPServer
    thread: threading.Thread

    def expect_json(
        self,
        path: str,
        payload: Any,
        *,
        method: str = "GET",
        status: int = 200,
        headers: dict[str, str] | None = None,
    ) -> None:
        if not path.startswith("/"):
            path = f"/{path}"
        response_headers = {"Content-Type": "application/json"}
        if headers:
            response_headers.update(headers)
        body = json.dumps(payload).encode("utf-8")
        self.server.routes[(method.upper(), path)] = (status, response_headers, body)

    def url_for(self, path: str) -> str:
        if not path.startswith("/"):
            path = f"/{path}"
        host, port = self.server.server_address
        return f"http://{host}:{port}{path}"

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()


@pytest.fixture()
def httpserver() -> _MiniHTTPServer:
    server = _ThreadingHTTPServer(("127.0.0.1", 0), _RequestHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    mini_server = _MiniHTTPServer(server=server, thread=thread)
    try:
        yield mini_server
    finally:
        mini_server.stop()


def test_uniprot_pipeline_integration(httpserver: _MiniHTTPServer, tmp_path: Path) -> None:
    config = load_config("configs/pipelines/uniprot.yaml")
    config.sources["uniprot"].base_url = httpserver.url_for("/uniprotkb")
    config.sources["uniprot_idmapping"].enabled = False
    config.sources["uniprot_orthologs"].enabled = False

    payload = {
        "results": [
            {
                "primaryAccession": "P12345",
                "genes": [{"geneName": {"value": "ABC1"}}],
                "proteinDescription": {"recommendedName": {"fullName": {"value": "Protein ABC"}}},
                "sequence": {"length": 512},
                "organism": {"taxonId": 9606, "scientificName": "Homo sapiens"},
            }
        ]
    }
    httpserver.expect_json("/uniprotkb/search", payload)

    pipeline = UniProtPipeline(config, "integration-test")

    original_additional = pipeline.add_additional_table

    def _add_csv_only(
        self,
        name: str,
        frame: pd.DataFrame | None,
        *,
        relative_path: Path | str | None = None,
        formats: Sequence[str] | None = None,
    ) -> None:
        original_additional(
            name,
            frame,
            relative_path=relative_path,
            formats=("csv",),
        )

    pipeline.add_additional_table = types.MethodType(_add_csv_only, pipeline)

    input_path = tmp_path / "uniprot.csv"
    pd.DataFrame({"uniprot_accession": ["P12345"]}).to_csv(input_path, index=False)
    output_path = tmp_path / "output.csv"

    artifacts = pipeline.run(output_path, input_file=input_path)

    dataset = pd.read_csv(output_path)
    assert not dataset.empty
    assert "uniprot_canonical_accession" in dataset.columns
    assert artifacts.dataset == output_path
