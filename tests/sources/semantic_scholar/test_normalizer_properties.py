"""Property-based tests for the Semantic Scholar normalizer."""

from __future__ import annotations

import string

import pytest

hypothesis = pytest.importorskip("hypothesis")
from hypothesis import given
from hypothesis import strategies as st
from hypothesis.strategies import SearchStrategy

from tests.sources.semantic_scholar import SemanticScholarAdapterTestCase


def doi_strings() -> SearchStrategy[str]:
    prefixes = st.sampled_from(["", "https://doi.org/", "http://dx.doi.org/", "doi:", "DOI: "])
    registrant = st.integers(min_value=1000, max_value=999999)
    suffix = st.text(
        alphabet=string.ascii_letters + string.digits + string.punctuation.replace(" ", ""),
        min_size=1,
        max_size=20,
    )
    return st.builds(lambda prefix, reg, tail: f"{prefix}10.{reg}/{tail}", prefixes, registrant, suffix)


class TestSemanticScholarNormalizerProperties(SemanticScholarAdapterTestCase):
    """Ensure DOI normalisation is resilient to noisy input."""

    @given(doi_value=doi_strings())
    def test_doi_normalization_is_canonical(self, doi_value: str) -> None:
        """``normalize_record`` should emit canonical DOI strings."""

        record = {
            "paperId": "hash-identifier",
            "externalIds": {"DOI": doi_value},
            "title": "Example",
            "authors": [],
        }

        normalized = self.adapter.normalize_record(record)
        doi_clean = normalized.get("doi_clean")

        if doi_clean:
            assert doi_clean.startswith("10.")
            assert normalized.get("semantic_scholar_doi") == doi_clean
        else:
            assert normalized.get("semantic_scholar_doi") is None
