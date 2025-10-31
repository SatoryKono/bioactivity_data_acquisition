"""End-to-end validation for document merge policy behaviour."""

from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

from bioetl.config.loader import load_config
from bioetl.config.paths import get_config_path
from bioetl.pipelines.document import DocumentPipeline
from bioetl.sources.document.pipeline import ExternalEnrichmentResult


pytestmark = pytest.mark.integration


@pytest.fixture
def document_config_path() -> str:
    """Return the test document pipeline configuration path."""

    return get_config_path("profiles/document_test.yaml")


class _StubAdapter(SimpleNamespace):
    """Adapter shim returning a predetermined dataframe."""

    dataframe: pd.DataFrame

    def process(self, identifiers: list[str]) -> pd.DataFrame:  # pragma: no cover - simple shim
        self.last_identifiers = identifiers
        return self.dataframe.copy()


def test_enrichment_merges_independent_sources(monkeypatch: pytest.MonkeyPatch, document_config_path: str) -> None:
    """Ensure enrichment aggregates disparate sources using the merge policy."""

    config = load_config(document_config_path)
    monkeypatch.setattr(DocumentPipeline, "_init_external_adapters", lambda self: None)
    pipeline = DocumentPipeline(config, run_id="merge-policy-test")

    chembl_df = pd.DataFrame(
        {
            "chembl_doi": ["10.1000/example"],
            "chembl_pmid": ["123456"],
            "chembl_title": ["ChEMBL Baseline"],
            "chembl_abstract": ["Baseline abstract"],
        }
    )

    pubmed_df = pd.DataFrame(
        {
            "pubmed_pmid": ["123456"],
            "pubmed_article_title": ["PubMed Overrides"],
            "pubmed_abstract": ["PubMed abstract"],
        }
    )
    crossref_df = pd.DataFrame(
        {
            "doi_clean": ["10.1000/example"],
            "title": ["Crossref Backup"],
            "journal": ["Crossref Journal"],
        }
    )
    openalex_df = pd.DataFrame(
        {
            "doi_clean": ["10.1000/example"],
            "title": ["OpenAlex Signal"],
            "is_oa": [True],
            "oa_status": ["gold"],
        }
    )
    semantic_df = pd.DataFrame(
        {
            "doi_clean": ["10.1000/example"],
            "citation_count": [42],
            "influential_citations": [5],
        }
    )

    pipeline.external_adapters = {
        "pubmed": _StubAdapter(dataframe=pubmed_df),
        "crossref": _StubAdapter(dataframe=crossref_df),
        "openalex": _StubAdapter(dataframe=openalex_df),
        "semantic_scholar": _StubAdapter(dataframe=semantic_df),
    }

    result: ExternalEnrichmentResult = pipeline._enrich_with_external_sources(chembl_df)  # noqa: SLF001
    enriched = result.dataframe

    assert not result.has_errors()
    assert result.status == "completed"

    assert enriched["title"].iloc[0] == "PubMed Overrides"
    assert enriched["title_source"].iloc[0] == "pubmed"
    assert enriched["oa_status"].iloc[0] == "gold"
    assert enriched["oa_status_source"].iloc[0] == "openalex"
    assert enriched["citation_count"].iloc[0] == 42
    assert enriched["citation_count_source"].iloc[0] == "semantic_scholar"
