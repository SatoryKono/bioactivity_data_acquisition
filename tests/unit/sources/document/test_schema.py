"""Schema-like expectations for document field precedence."""

from __future__ import annotations

from bioetl.sources.document.merge.policy import FIELD_PRECEDENCE


def test_field_precedence_includes_core_sources() -> None:
    """Ensure precedence rules cover the key bibliographic fields."""

    assert "doi_clean" in FIELD_PRECEDENCE
    assert ("crossref", "crossref_doi") in FIELD_PRECEDENCE["doi_clean"]
    assert ("pubmed", "pubmed_pmid") in FIELD_PRECEDENCE["pmid"]
    assert ("chembl", "chembl_title") in FIELD_PRECEDENCE["title"]
