"""Property-based tests for the OpenAlex normalizer."""

from __future__ import annotations

import string

import pytest

hypothesis = pytest.importorskip("hypothesis")
from hypothesis import given
from hypothesis import strategies as st
from hypothesis.strategies import SearchStrategy

from tests.unit.sources.openalex import OpenAlexAdapterTestCase


def doi_strings() -> SearchStrategy[str]:
    """Generate DOI strings with assorted prefixes and noise."""

    prefixes = st.sampled_from(["", "https://doi.org/", "http://doi.org/", "doi:", "DOI: "])
    registrant = st.integers(min_value=1000, max_value=999999)
    suffix = st.text(
        alphabet=string.ascii_letters + string.digits + string.punctuation.replace(" ", ""),
        min_size=1,
        max_size=20,
    )

    return st.builds(lambda prefix, reg, tail: f"{prefix}10.{reg}/{tail}", prefixes, registrant, suffix)


class TestOpenAlexNormalizerProperties(OpenAlexAdapterTestCase):
    """Property-based checks ensuring DOI normalisation is stable."""

    @given(doi_value=doi_strings())
    def test_doi_normalization_is_canonical(self, doi_value: str) -> None:
        """``normalize_record`` should emit a canonical DOI independent of formatting."""

        record = {
            "id": "https://openalex.org/W1234567890",
            "doi": doi_value,
            "title": "Noise Resistant Normalisation",
            "authorships": [],
            "primary_location": {"source": {}},
            "concepts": [],
            "open_access": {},
            "ids": {},
        }

        normalized = self.adapter.normalize_record(record)
        doi_clean = normalized.get("doi_clean")

        if doi_clean:
            assert doi_clean.startswith("10.")
            assert normalized.get("openalex_doi") == doi_clean
            assert normalized.get("openalex_doi_clean") == doi_clean
        else:
            assert normalized.get("openalex_doi") is None
