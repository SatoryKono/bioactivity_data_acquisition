"""Unit tests for merge_with_precedence utilities."""

import pandas as pd

from bioetl.pipelines.document_enrichment import merge_with_precedence


def test_merge_with_precedence_prioritizes_sources():
    """Merge should honor field precedence and record sources."""
    chembl_df = pd.DataFrame(
        {
            "document_chembl_id": ["CHEMBL1"],
            "chembl_pmid": [123],
            "chembl_title": ["ChEMBL Title"],
            "chembl_abstract": ["ChEMBL Abstract"],
            "chembl_journal": ["ChEMBL Journal"],
            "chembl_authors": ["ChEMBL Authors"],
            "chembl_doi": ["10.1000/chembl"],
            "chembl_year": [1999],
            "chembl_volume": ["5"],
            "chembl_issue": ["1"],
        }
    )

    pubmed_df = pd.DataFrame(
        {
            "pubmed_pmid": [123],
            "title": ["PubMed Title"],
            "abstract": ["PubMed Abstract"],
            "journal": ["PubMed Journal"],
            "journal_abbrev": ["PubMed J."],
            "authors": ["PubMed Authors"],
            "doi_clean": ["10.1000/pubmed"],
            "pubmed_doi": ["10.1000/pubmed"],
            "volume": ["20"],
            "issue": ["3"],
            "first_page": ["101"],
            "last_page": ["110"],
            "year": [2001],
            "issn_print": ["0000-1111"],
            "issn_electronic": ["1111-2222"],
            "mesh_descriptors": ["Term1 | Term2"],
            "mesh_qualifiers": ["Qualifier1"],
            "chemical_list": ["Chem1 | Chem2"],
        }
    )

    crossref_df = pd.DataFrame(
        {
            "doi_clean": ["10.1000/chembl"],
            "crossref_doi": ["10.1000/chembl"],
            "title": ["Crossref Title"],
            "journal": ["Crossref Journal"],
            "authors": ["Crossref Authors"],
            "volume": ["30"],
            "issue": ["4"],
            "first_page": ["201"],
            "issn_print": ["2222-3333"],
            "issn_electronic": ["3333-4444"],
            "year": [2002],
        }
    )

    openalex_df = pd.DataFrame(
        {
            "doi_clean": ["10.1000/chembl"],
            "openalex_doi": ["10.1000/chembl"],
            "title": ["OpenAlex Title"],
            "journal": ["OpenAlex Journal"],
            "year": [2003],
            "authors": ["OpenAlex Authors"],
            "concepts_top3": [["Concept A", "Concept B"]],
            "is_oa": [True],
            "oa_status": ["gold"],
            "oa_url": ["https://example.org/open"],
            "openalex_pmid": [123],
        }
    )

    semantic_df = pd.DataFrame(
        {
            "doi_clean": ["10.1000/chembl"],
            "title": ["Semantic Scholar Title"],
            "abstract": ["Semantic Abstract"],
            "journal": ["Semantic Journal"],
            "year": [2004],
            "citation_count": [42],
            "influential_citations": [7],
            "fields_of_study": [["Biology", "Chemistry"]],
            "authors": ["Semantic Authors"],
            "pubmed_id": [456],
        }
    )

    merged = merge_with_precedence(
        chembl_df,
        pubmed_df=pubmed_df,
        crossref_df=crossref_df,
        openalex_df=openalex_df,
        semantic_scholar_df=semantic_df,
    )

    assert merged.loc[0, "title"] == "PubMed Title"
    assert merged.loc[0, "title_source"] == "pubmed"
    assert merged.loc[0, "abstract"] == "PubMed Abstract"
    assert merged.loc[0, "abstract_source"] == "pubmed"
    assert merged.loc[0, "journal"] == "PubMed Journal"
    assert merged.loc[0, "journal_source"] == "pubmed"
    assert merged.loc[0, "journal_abbrev"] == "PubMed J."
    assert merged.loc[0, "journal_abbrev_source"] == "pubmed"
    assert merged.loc[0, "authors"] == "PubMed Authors"
    assert merged.loc[0, "authors_source"] == "pubmed"
    assert int(merged.loc[0, "pmid"]) == 123
    assert merged.loc[0, "pmid_source"] == "pubmed"
    assert merged.loc[0, "doi_clean"] == "10.1000/chembl"
    assert merged.loc[0, "doi_clean_source"] == "crossref"
    assert int(merged.loc[0, "year"]) == 2001
    assert merged.loc[0, "year_source"] == "pubmed"
    assert merged.loc[0, "volume"] == "20"
    assert merged.loc[0, "volume_source"] == "pubmed"
    assert merged.loc[0, "issue"] == "3"
    assert merged.loc[0, "issue_source"] == "pubmed"
    assert merged.loc[0, "first_page"] == "101"
    assert merged.loc[0, "first_page_source"] == "pubmed"
    assert merged.loc[0, "last_page"] == "110"
    assert merged.loc[0, "last_page_source"] == "pubmed"
    assert merged.loc[0, "issn_print"] == "2222-3333"
    assert merged.loc[0, "issn_print_source"] == "crossref"
    assert merged.loc[0, "issn_electronic"] == "3333-4444"
    assert merged.loc[0, "issn_electronic_source"] == "crossref"
    assert bool(merged.loc[0, "is_oa"]) is True
    assert merged.loc[0, "is_oa_source"] == "openalex"
    assert merged.loc[0, "oa_status"] == "gold"
    assert merged.loc[0, "oa_status_source"] == "openalex"
    assert merged.loc[0, "oa_url"] == "https://example.org/open"
    assert merged.loc[0, "oa_url_source"] == "openalex"
    assert int(merged.loc[0, "citation_count"]) == 42
    assert merged.loc[0, "citation_count_source"] == "semantic_scholar"
    assert int(merged.loc[0, "influential_citations"]) == 7
    assert merged.loc[0, "influential_citations_source"] == "semantic_scholar"
    assert merged.loc[0, "fields_of_study"] == "Biology; Chemistry"
    assert merged.loc[0, "fields_of_study_source"] == "semantic_scholar"
    assert merged.loc[0, "concepts_top3"] == "Concept A; Concept B"
    assert merged.loc[0, "concepts_top3_source"] == "openalex"
    assert merged.loc[0, "mesh_terms"] == "Term1 | Term2"
    assert merged.loc[0, "mesh_terms_source"] == "pubmed"
    assert merged.loc[0, "chemicals"] == "Chem1 | Chem2"
    assert merged.loc[0, "chemicals_source"] == "pubmed"
    assert bool(merged.loc[0, "conflict_doi"]) is True
    assert bool(merged.loc[0, "conflict_pmid"]) is True


def test_merge_with_precedence_handles_missing_sources():
    """When only ChEMBL data is provided, resolved fields fall back to ChEMBL."""
    chembl_df = pd.DataFrame(
        {
            "document_chembl_id": ["CHEMBL2"],
            "chembl_pmid": [789],
            "chembl_title": ["Solo Title"],
            "chembl_abstract": ["Solo Abstract"],
            "chembl_journal": ["Solo Journal"],
            "chembl_authors": ["Solo Authors"],
            "chembl_doi": ["10.2000/solo"],
            "chembl_year": [2005],
            "chembl_volume": ["12"],
            "chembl_issue": ["2"],
        }
    )

    merged = merge_with_precedence(chembl_df)

    assert merged.loc[0, "title"] == "Solo Title"
    assert merged.loc[0, "title_source"] == "chembl"
    assert merged.loc[0, "abstract"] == "Solo Abstract"
    assert merged.loc[0, "abstract_source"] == "chembl"
    assert merged.loc[0, "journal"] == "Solo Journal"
    assert merged.loc[0, "journal_source"] == "chembl"
    assert merged.loc[0, "authors"] == "Solo Authors"
    assert merged.loc[0, "authors_source"] == "chembl"
    assert int(merged.loc[0, "pmid"]) == 789
    assert merged.loc[0, "pmid_source"] == "chembl"
    assert merged.loc[0, "doi_clean"] == "10.2000/solo"
    assert merged.loc[0, "doi_clean_source"] == "chembl"
    assert bool(merged.loc[0, "conflict_doi"]) is False
    assert bool(merged.loc[0, "conflict_pmid"]) is False
    assert pd.isna(merged.loc[0, "concepts_top3"])
    assert pd.isna(merged.loc[0, "mesh_terms"])
