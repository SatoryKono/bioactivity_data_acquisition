"""Unit tests for merge policy strategies."""

from __future__ import annotations

import pandas as pd

from bioetl.sources.document.merge.policy import (
    MergeCandidate,
    MergePolicy,
    MergeRule,
    MergeStrategy,
)


def test_prefer_source_records_extras() -> None:
    df = pd.DataFrame({"chembl_doi": ["10.1/chembl"], "pubmed_doi": ["10.2/pubmed"]})

    policy = MergePolicy(
        [
            MergeRule(
                field="doi_clean",
                strategy=MergeStrategy.PREFER_SOURCE,
                candidates=(
                    MergeCandidate("chembl", "chembl_doi"),
                    MergeCandidate("pubmed", "pubmed_doi"),
                ),
            )
        ]
    )

    result = policy.apply(df)

    assert result.loc[0, "doi_clean"] == "10.1/chembl"
    assert result.loc[0, "doi_clean_source"] == "chembl"
    extras = result.loc[0, "doi_clean_extras"]
    assert extras == [
        {"source": "pubmed", "column": "pubmed_doi", "value": "10.2/pubmed"}
    ]


def test_prefer_fresh_uses_latest_timestamp() -> None:
    df = pd.DataFrame(
        {
            "chembl_title": ["Legacy"],
            "chembl_updated": ["2024-01-01T00:00:00Z"],
            "pubmed_title": ["Fresh"],
            "pubmed_updated": ["2024-02-01T00:00:00Z"],
        }
    )

    policy = MergePolicy(
        [
            MergeRule(
                field="title",
                strategy=MergeStrategy.PREFER_FRESH,
                candidates=(
                    MergeCandidate("chembl", "chembl_title", freshness_column="chembl_updated"),
                    MergeCandidate("pubmed", "pubmed_title", freshness_column="pubmed_updated"),
                ),
            )
        ]
    )

    result = policy.apply(df)

    assert result.loc[0, "title"] == "Fresh"
    assert result.loc[0, "title_source"] == "pubmed"
    extras = result.loc[0, "title_extras"]
    assert isinstance(extras, list)
    assert extras[0]["source"] == "chembl"
    assert extras[0]["value"] == "Legacy"
    assert extras[0]["freshness"].startswith("2024-01-01")


def test_concat_unique_deduplicates_values() -> None:
    df = pd.DataFrame(
        {
            "pubmed_authors": [["Alice", "Bob"]],
            "crossref_authors": [["Bob", "Carol"]],
        }
    )

    policy = MergePolicy(
        [
            MergeRule(
                field="authors",
                strategy=MergeStrategy.CONCAT_UNIQUE,
                candidates=(
                    MergeCandidate("pubmed", "pubmed_authors"),
                    MergeCandidate("crossref", "crossref_authors"),
                ),
            )
        ]
    )

    result = policy.apply(df)

    assert result.loc[0, "authors"] == "Alice; Bob; Carol"
    assert result.loc[0, "authors_source"] == "pubmed"
    assert "authors_extras" not in result.columns or pd.isna(result.loc[0, "authors_extras"])


def test_score_based_prefers_highest_score() -> None:
    df = pd.DataFrame(
        {
            "openalex_metric": [75],
            "semantic_metric": [125],
        }
    )

    policy = MergePolicy(
        [
            MergeRule(
                field="citation_count",
                strategy=MergeStrategy.SCORE_BASED,
                candidates=(
                    MergeCandidate("openalex", "openalex_metric", score_column="openalex_metric"),
                    MergeCandidate("semantic_scholar", "semantic_metric", score_column="semantic_metric"),
                ),
            )
        ]
    )

    result = policy.apply(df)

    assert result.loc[0, "citation_count"] == 125
    assert result.loc[0, "citation_count_source"] == "semantic_scholar"
    extras = result.loc[0, "citation_count_extras"]
    assert extras == [
        {
            "source": "openalex",
            "column": "openalex_metric",
            "value": 75,
            "score": 75.0,
        }
    ]

