"""IUPHAR pipeline end-to-end tests."""

from __future__ import annotations

from pathlib import Path

import pytest
from pytest_httpserver import HTTPServer

from bioetl.config.loader import load_config
from bioetl.sources.iuphar.pipeline import GtpIupharPipeline


@pytest.fixture(autouse=True)
def _ensure_iuphar_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("IUPHAR_API_KEY", "test-key")


def _build_response() -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    targets = [
        {
            "targetId": 1,
            "name": "Integration Target",
            "synonyms": "Alpha|Beta",
            "abbreviation": "IT",
            "annotationStatus": "CURATED",
            "geneSymbol": "ITG",
            "familyIds": [11],
        }
    ]
    families = [
        {"familyId": 1, "name": "GPCRs", "parentFamilyIds": []},
        {"familyId": 10, "name": "Class A GPCRs", "parentFamilyIds": [1]},
        {"familyId": 11, "name": "Adenosine receptors", "parentFamilyIds": [10]},
    ]
    return targets, families


def test_iuphar_pipeline_end_to_end(httpserver: HTTPServer, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    targets, families = _build_response()
    httpserver.expect_request("/targets").respond_with_json(targets)
    httpserver.expect_request("/targets/families").respond_with_json(families)

    config = load_config(
        "configs/pipelines/iuphar.yaml",
        overrides={"sources": {"iuphar": {"base_url": httpserver.url_for("")}}},
    )

    pipeline = GtpIupharPipeline(config, run_id="integration-test")

    extracted = pipeline.extract()
    assert not extracted.empty

    transformed = pipeline.transform(extracted)
    assert transformed.loc[0, "iuphar_type"] == "GPCRs"

    validated = pipeline.validate(transformed)
    assert not validated.empty

    httpserver.check_assertions()
