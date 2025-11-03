"""Tests for bibliographic normalization helpers."""

import pytest

import bioetl.normalizers  # noqa: F401  # Ensure normalizers are registered
from bioetl.normalizers.bibliography import (
    normalize_authors,
    normalize_doi,
    normalize_title,
)


@pytest.mark.parametrize(
    "value,expected",
    [
        ("10.1000/XYZ123", "10.1000/xyz123"),
        ("https://doi.org/10.1000/XYZ123", "10.1000/xyz123"),
        (["", "DOI:10.1000/ABC"], "10.1000/abc"),
        (None, None),
    ],
)
def test_normalize_doi_variants(value, expected):
    """DOIs from various formats are normalized consistently."""

    assert normalize_doi(value) == expected


@pytest.mark.parametrize(
    "value,expected",
    [
        ("  Example Title  ", "Example Title"),
        (["", "Second"], "Second"),
        (None, None),
    ],
)
def test_normalize_title_variants(value, expected):
    """Titles from strings or sequences are normalized using string normalizer."""

    assert normalize_title(value) == expected


@pytest.mark.parametrize(
    "value,expected",
    [
        (
            [
                {"given": "Jane", "family": "Doe"},
                {"family": "Smith", "given": "John"},
            ],
            "Doe, Jane; Smith, John",
        ),
        (
            [
                {"author": {"display_name": "Alice Example"}},
                {"author": {"display_name": "Bob Example"}},
            ],
            "Alice Example; Bob Example",
        ),
        (
            [{"name": "Charlie"}, "Delta"],
            "Charlie; Delta",
        ),
        ("Epsilon", "Epsilon"),
        ([], None),
    ],
)
def test_normalize_authors_variants(value, expected):
    """Authors from multiple structures collapse to a deterministic string."""

    assert normalize_authors(value) == expected
