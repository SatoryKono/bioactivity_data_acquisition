"""Tests for external request helpers used by the document pipeline."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pandas as pd

from bioetl.sources.chembl.document.request import (
    collect_enrichment_metrics,
    run_enrichment_requests,
)


class StubAdapter:
    """Adapter stub mimicking the pipeline adapter contract."""

    def __init__(
        self,
        result: pd.DataFrame | Exception,
        fallback_result: pd.DataFrame | Exception | None = None,
    ):
        self.result = result
        self.fallback_result = fallback_result
        self.calls: list[Sequence[str]] = []
        self.fallback_calls: list[Sequence[str]] = []

    def process(self, ids: Sequence[str]) -> pd.DataFrame:
        self.calls.append(tuple(ids))
        if isinstance(self.result, Exception):
            raise self.result
        return self.result

    def process_titles(self, titles: Sequence[str]) -> pd.DataFrame:
        return self.process(titles)

    def process_with_fallback(self, ids: Sequence[str]) -> pd.DataFrame:
        self.fallback_calls.append(tuple(ids))
        fallback = self.fallback_result
        if isinstance(fallback, Exception):
            raise fallback
        if fallback is None:
            return pd.DataFrame()
        return fallback


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

    returned_pubmed, returned_crossref, openalex_df, semantic_df, errors, coverage = results

    assert errors == {}
    assert returned_pubmed is pubmed_df
    assert returned_crossref is crossref_df
    assert openalex_df is None
    assert semantic_df is None
    assert coverage["pubmed"]["coverage"] == 1.0
    assert coverage["crossref"]["coverage"] == 1.0


def test_run_enrichment_requests_captures_errors() -> None:
    """Adapter exceptions should be collected without bubbling up."""

    boom = RuntimeError("boom")
    adapters: dict[str, Any] = {
        "pubmed": StubAdapter(boom),
        "semantic_scholar": StubAdapter(pd.DataFrame({"title": ["x"]})),
    }

    _, _, _, semantic_df, errors, coverage = run_enrichment_requests(
        adapters,
        pmids=["1"],
        dois=[],
        titles=["x"],
        timeout=1.0,
    )

    assert "pubmed" in errors
    assert semantic_df is not None
    assert coverage["pubmed"]["coverage"] == 0.0
    assert coverage["pubmed"]["missing_ids"] == ["1"]


def test_collect_enrichment_metrics_reports_rows_and_errors() -> None:
    """Metrics helper should track row counts and failures per adapter."""

    frames = {
        "pubmed": pd.DataFrame({"pmid": [1, 2]}),
        "crossref": None,
    }
    errors = {"crossref": "timeout"}
    coverage = {
        "pubmed": {"requested": 2, "matched": 2, "coverage": 1.0, "missing_ids": [], "fallback_attempted": False},
        "crossref": {"requested": 1, "matched": 0, "coverage": 0.0, "missing_ids": ["missing"], "fallback_attempted": True},
    }

    metrics = collect_enrichment_metrics(frames, errors, coverage)

    assert metrics.loc[metrics["source"] == "pubmed", "rows"].item() == 2
    crossref_row = metrics.loc[metrics["source"] == "crossref"].iloc[0]
    assert crossref_row["rows"] == 0
    assert crossref_row["status"] == "failed"
    assert crossref_row["missing_count"] == 1
    assert bool(crossref_row["fallback_attempted"]) is True


def test_run_enrichment_requests_recovers_via_fallback() -> None:
    """Fallback-enabled adapters should close identifier gaps."""

    initial = pd.DataFrame({"pubmed_pmid": [1]})
    fallback = pd.DataFrame({"pubmed_pmid": [2]})
    adapters: dict[str, Any] = {
        "pubmed": StubAdapter(initial, fallback_result=fallback),
    }

    _, _, _, _, errors, coverage = run_enrichment_requests(
        adapters,
        pmids=["1", "2"],
        dois=[],
        titles=[],
    )

    assert errors == {}
    pubmed_coverage = coverage["pubmed"]
    assert pubmed_coverage["coverage"] == 1.0
    assert pubmed_coverage["recovered_ids"] == ["2"]
    assert pubmed_coverage["fallback_attempted"] is True


def test_run_enrichment_requests_reports_missing_after_failed_fallback() -> None:
    """Missing identifiers should be surfaced when fallback cannot recover them."""

    initial = pd.DataFrame({"pubmed_pmid": [1]})
    adapters: dict[str, Any] = {
        "pubmed": StubAdapter(initial, fallback_result=pd.DataFrame()),
    }

    _, _, _, _, errors, coverage = run_enrichment_requests(
        adapters,
        pmids=["1", "2"],
        dois=[],
        titles=[],
    )

    assert "pubmed" in errors
    assert "missing_identifiers" in errors["pubmed"]
    pubmed_coverage = coverage["pubmed"]
    assert pubmed_coverage["coverage"] == 0.5
    assert pubmed_coverage["missing_ids"] == ["2"]
    assert pubmed_coverage["fallback_attempted"] is True
