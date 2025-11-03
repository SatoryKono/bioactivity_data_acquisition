"""Property-based tests for the TestItem request builder pagination logic."""

from __future__ import annotations

from urllib.parse import parse_qs, urlparse

import pytest

pytest.importorskip("hypothesis")

from hypothesis import given, settings
from hypothesis import strategies as st

from bioetl.core.api_client import APIConfig, UnifiedAPIClient
from bioetl.sources.chembl.testitem.request import TestItemRequestBuilder

pytestmark = pytest.mark.property


_API_CLIENT = UnifiedAPIClient(APIConfig(name="chembl", base_url="https://example.org/api"))


def _id_strings() -> st.SearchStrategy[str]:
    """Generate ChEMBL-like identifiers with optional whitespace noise."""

    base = st.text(alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", min_size=1, max_size=8)
    padding = st.text(alphabet=" \t", max_size=2)
    return st.builds(lambda left, body, right: f"{left}{body}{right}", padding, base, padding)


def _builder(batch_size: int, max_url_length: int | None) -> TestItemRequestBuilder:
    return TestItemRequestBuilder(
        api_client=_API_CLIENT,
        batch_size=batch_size,
        max_url_length=max_url_length,
    )


@given(
    st.lists(st.one_of(_id_strings(), st.just(""), st.none()), max_size=30),
    st.integers(min_value=1, max_value=10),
    st.one_of(st.none(), st.integers(min_value=32, max_value=200)),
)
@settings(max_examples=200, deadline=None)
def test_iter_batches_preserves_all_non_empty_ids(
    molecule_ids: list[str | None], batch_size: int, max_url_length: int | None
) -> None:
    """Batch iteration must keep every non-empty identifier exactly once."""

    builder = _builder(batch_size=batch_size, max_url_length=max_url_length)
    batches = builder.iter_batches(list(molecule_ids))

    flattened: list[str] = [identifier for batch in batches for identifier in batch]
    expected = [identifier for identifier in molecule_ids if identifier and identifier.strip()]

    assert flattened == expected
    for batch in batches:
        assert len(batch) <= batch_size
        assert all(identifier and identifier.strip() for identifier in batch)


@given(
    st.lists(_id_strings(), min_size=1, max_size=25),
    st.integers(min_value=1, max_value=10),
    st.integers(min_value=40, max_value=200),
)
@settings(max_examples=200, deadline=None)
def test_split_respects_url_length_for_multi_id_batches(
    molecule_ids: list[str], batch_size: int, max_url_length: int
) -> None:
    """Batches containing multiple IDs must produce URLs within the configured limit."""

    builder = _builder(batch_size=batch_size, max_url_length=max_url_length)
    batches = builder.iter_batches(molecule_ids)

    for batch in batches:
        if len(batch) > 1:
            url_length = len(builder.build_url(batch))
            assert url_length <= max_url_length


@given(
    st.lists(_id_strings(), min_size=1, max_size=25),
    st.integers(min_value=1, max_value=10),
)
@settings(max_examples=200, deadline=None)
def test_build_url_includes_limit_parameter(
    molecule_ids: list[str], batch_size: int
) -> None:
    """Generated URLs must include the limit parameter capped by the batch size."""

    builder = _builder(batch_size=batch_size, max_url_length=None)
    url = builder.build_url(molecule_ids)

    limit = min(len(molecule_ids), batch_size)
    assert f"limit={limit}" in url

    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    assert params.get("molecule_chembl_id__in") == [",".join(molecule_ids)]
