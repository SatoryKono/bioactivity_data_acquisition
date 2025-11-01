"""Tests for external request helpers used by the document pipeline."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pandas as pd

from bioetl.adapters import AdapterFetchError
from bioetl.sources.chembl.document.request import (
    collect_enrichment_metrics,
    run_enrichment_requests,
)


class StubAdapter:
    """Adapter stub mimicking the pipeline adapter contract."""

    def __init__(self, result: pd.DataFrame | Exception):
        self.result = result
        self.calls: list[Sequence[str]] = []

    def process(self, ids: Sequence[str]) -> pd.DataFrame:
        self.calls.append(tuple(ids))
        if isinstance(self.result, Exception):
            raise self.result
        return self.result

    def process_titles(self, titles: Sequence[str]) -> pd.DataFrame:
        return self.process(titles)


def test_run_enrichment_requests_collects_successful_results() -> None:
    """Adapters returning dataframes should be forwarded in the response tuple."""

    pubmed_df = pd.DataFrame({"pmid": ["1"]})
    crossref_df = pd.DataFrame({"doi": ["10.1/example"]})
    adapters: dict[str, Any] = {
        "pubmed": StubAdapter(pubmed_df),
        "crossref": StubAdapter(crossref_df),
    }

    results = run_enrichment_requests(
        adapters,
        pmids=["1"],
        dois=["10.1/example"],
        titles=[],
    )

    returned_pubmed, returned_crossref, openalex_df, semantic_df, errors = results

    assert errors == {}
    assert returned_pubmed is pubmed_df
    assert returned_crossref is crossref_df
    assert openalex_df is None
    assert semantic_df is None


def test_run_enrichment_requests_captures_errors() -> None:
    """Adapter exceptions should be collected without bubbling up."""

    boom = AdapterFetchError("boom", failed_ids=["1"])
    adapters: dict[str, Any] = {
        "pubmed": StubAdapter(boom),
        "semantic_scholar": StubAdapter(pd.DataFrame({"title": ["x"]})),
    }

    _, _, _, semantic_df, errors = run_enrichment_requests(
        adapters,
        pmids=["1"],
        dois=[],
        titles=["x"],
        timeout=1.0,
    )

    assert errors == {"pubmed": str(boom)}
    assert semantic_df is not None


def test_run_enrichment_requests_error_marks_failed_status() -> None:
    """Adapter fetch errors should propagate into QC metrics as failures."""

    boom = AdapterFetchError("pubmed down", failed_ids=["1", "2"])
    adapters: dict[str, Any] = {
        "pubmed": StubAdapter(boom),
        "crossref": StubAdapter(pd.DataFrame({"doi": ["10.1/foo"]})),
    }

    pubmed_df, crossref_df, *_rest, errors = run_enrichment_requests(
        adapters,
        pmids=["1", "2"],
        dois=["10.1/foo"],
        titles=[],
    )

    assert pubmed_df is None
    assert crossref_df is not None
    metrics = collect_enrichment_metrics({"pubmed": pubmed_df, "crossref": crossref_df}, errors)
    pubmed_status = metrics.loc[metrics["source"] == "pubmed", "status"].item()
    assert pubmed_status == "failed"


def test_collect_enrichment_metrics_reports_rows_and_errors() -> None:
    """Metrics helper should track row counts and failures per adapter."""

    frames = {
        "pubmed": pd.DataFrame({"pmid": [1, 2]}),
        "crossref": None,
    }
    errors = {"crossref": "timeout"}

    metrics = collect_enrichment_metrics(frames, errors)

    assert metrics.loc[metrics["source"] == "pubmed", "rows"].item() == 2
    crossref_row = metrics.loc[metrics["source"] == "crossref"].iloc[0]
    assert crossref_row["rows"] == 0
    assert crossref_row["status"] == "failed"
