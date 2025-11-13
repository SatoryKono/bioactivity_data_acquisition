"""Unit tests for hashing utilities."""

from __future__ import annotations

import pytest

from bioetl.core import compute_hash, hash_from_mapping


@pytest.mark.unit
def test_compute_hash_normalises_strings() -> None:
    reference = compute_hash(["value", "example"])
    with_whitespace = compute_hash([" value ", "EXAMPLE"])

    assert reference == with_whitespace


@pytest.mark.unit
def test_compute_hash_order_independent_for_sequences() -> None:
    hash_one = compute_hash([["b", "a", "c"]])
    hash_two = compute_hash([["c", "b", "a"]])

    assert hash_one == hash_two


@pytest.mark.unit
def test_hash_from_mapping_requires_fields() -> None:
    mapping = {"a": "value", "b": 1}

    with pytest.raises(KeyError, match="Fields required"):
        hash_from_mapping(mapping, ["missing"], algorithm="sha256")


@pytest.mark.unit
def test_hash_from_mapping_returns_digest() -> None:
    mapping = {"id": "ABC123", "value": 10}

    digest = hash_from_mapping(mapping, ["id", "value"], algorithm="sha256")

    assert len(digest) == 64
