"""Tests for the document merge policy and enrichment aggregation."""

from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

from bioetl.config.loader import load_config
from bioetl.config.paths import get_config_path
from bioetl.pipelines.document import DocumentPipeline
from bioetl.sources.document.merge.policy import merge_with_precedence
from bioetl.sources.document.pipeline import ExternalEnrichmentResult


def test_merge_policy_combines_multiple_sources() -> None:
    """The merge policy should consolidate bibliographic data across sources."""

    chembl_df = pd.DataFrame(
        {
            "document_chembl_id": ["CHEMBL123"],
            "chembl_doi": ["10.1000/example"],
            "chembl_pmid": [9876543],
            "chembl_title": ["Original"],
        }
    )
    pubmed_df = pd.DataFrame(
        {
            "pubmed_pmid": [9876543],
            "pubmed_doi": ["10.1000/example"],
            "pubmed_article_title": ["PubMed Title"],
            "pubmed_journal": ["Journal"],
        }
    )
    crossref_df = pd.DataFrame(
        {
            "crossref_doi": ["10.1000/example"],
            "crossref_title": ["Crossref Backup"],
            "crossref_journal": ["Crossref Journal"],
        }
    )

    merged = merge_with_precedence(chembl_df, pubmed_df=pubmed_df, crossref_df=crossref_df)

    assert merged.loc[0, "doi_clean"] == "10.1000/example"
    assert merged.loc[0, "crossref_title"] == "Crossref Backup"
    assert merged.loc[0, "crossref_journal"] == "Crossref Journal"
    assert merged.loc[0, "title"] == "PubMed Title"
    assert merged.loc[0, "journal"] == "Journal"
    assert merged.loc[0, "pmid"] == 9876543


@pytest.mark.integration
def test_enrichment_merges_independent_sources(monkeypatch: pytest.MonkeyPatch) -> None:
    """External adapters are merged using the policy when enriching documents."""

    config = load_config(get_config_path("profiles/document_test.yaml"))
    monkeypatch.setattr(DocumentPipeline, "_prepare_enrichment_adapters", lambda self: None)
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
            "crossref_doi": ["10.1000/example"],
            "crossref_title": ["Crossref Backup"],
            "crossref_journal": ["Crossref Journal"],
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

    class _StubAdapter(SimpleNamespace):
        def process(self, identifiers: list[str]) -> pd.DataFrame:  # pragma: no cover - simple stub
            self.last_identifiers = identifiers
            return self.dataframe.copy()

    pipeline.external_adapters = {
        "pubmed": _StubAdapter(dataframe=pubmed_df),
        "crossref": _StubAdapter(dataframe=crossref_df),
        "openalex": _StubAdapter(dataframe=openalex_df),
        "semantic_scholar": _StubAdapter(dataframe=semantic_df),
    }

    result: ExternalEnrichmentResult = pipeline._enrich_with_external_sources(chembl_df)
    enriched = result.dataframe

    assert not result.has_errors()
    assert result.status == "completed"

    assert enriched["title"].iloc[0] == "PubMed Overrides"
    assert enriched["title_source"].iloc[0] == "pubmed"
    assert enriched["oa_status"].iloc[0] == "gold"
    assert enriched["oa_status_source"].iloc[0] == "openalex"
    assert enriched["citation_count"].iloc[0] == 42
    assert enriched["citation_count_source"].iloc[0] == "semantic_scholar"
