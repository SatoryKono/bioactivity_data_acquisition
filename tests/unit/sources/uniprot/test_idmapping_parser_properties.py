"""Property-based tests for UniProt ID mapping parser helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import hypothesis.strategies as st
from hypothesis import given, settings

from bioetl.sources.uniprot.parser import parse_idmapping_results, parse_idmapping_status

BUCKET_SYNONYMS: dict[str, list[str]] = {
    "pending": ["pending", "queued"],
    "running": ["running", "active"],
    "finished": ["finished", "success", "completed", "complete"],
    "failed": ["failed", "error", "cancelled", "canceled"],
}


@st.composite
def whitespace(draw: st.DrawFn) -> str:
    return draw(st.text(min_size=0, max_size=3, alphabet=" \t"))


@st.composite
def bucketed_status_payloads(draw: st.DrawFn) -> tuple[dict[str, object], str]:
    expected = draw(st.sampled_from(sorted(BUCKET_SYNONYMS)))
    base = draw(st.sampled_from(BUCKET_SYNONYMS[expected]))
    transformer = draw(
        st.sampled_from(
            (
                str.lower,
                str.upper,
                str.title,
                lambda value: value,
            )
        )
    )
    prefix = draw(whitespace())
    suffix = draw(whitespace())
    key = draw(st.sampled_from(("jobStatus", "status")))
    payload: dict[str, object] = {
        key: f"{prefix}{transformer(base)}{suffix}",
        # include some noise to mimic API payloads
        "meta": draw(
            st.dictionaries(
                keys=st.text(min_size=0, max_size=5),
                values=st.recursive(
                    st.one_of(st.none(), st.text(max_size=5), st.integers(-3, 3)),
                    lambda children: st.lists(children, max_size=2)
                    | st.dictionaries(st.text(max_size=5), children, max_size=2),
                    max_leaves=5,
                ),
                max_size=2,
            )
        ),
    }
    return payload, expected


def nested_payloads() -> st.SearchStrategy[object]:
    leaf = st.one_of(
        st.none(),
        st.booleans(),
        st.integers(-5, 5),
        st.text(max_size=6),
        st.floats(allow_nan=False, allow_infinity=False),
    )
    return st.recursive(
        leaf,
        lambda children: st.lists(children, max_size=3)
        | st.dictionaries(st.text(max_size=6), children, max_size=3),
        max_leaves=6,
    )


@st.composite
def noisy_status_payloads(draw: st.DrawFn) -> Mapping[str, object]:
    payload = draw(
        st.dictionaries(
            keys=st.text(min_size=0, max_size=6),
            values=nested_payloads(),
            max_size=3,
        )
    )
    if draw(st.booleans()):
        key = draw(
            st.sampled_from(
                (
                    "jobStatus",
                    "status",
                    "JobStatus",
                    "STATUS",
                    "",
                    "state",
                )
            )
        )
        value = draw(
            st.one_of(
                st.text(max_size=6),
                st.just(""),
                st.none(),
                nested_payloads(),
            )
        )
        payload[key] = value
    return payload


@settings(max_examples=80, deadline=None)
@given(bucketed_status_payloads())
def test_property_idmapping_status_normalizes_known_variants(
    payload_expected: tuple[dict[str, object], str],
) -> None:
    payload, expected = payload_expected
    assert parse_idmapping_status(payload) == expected


@settings(max_examples=60, deadline=None)
@given(noisy_status_payloads())
def test_property_idmapping_status_handles_noisy_payloads(payload: Mapping[str, object]) -> None:
    result = parse_idmapping_status(payload)
    assert isinstance(result, str)


def truthy_values() -> st.SearchStrategy[object]:
    return st.one_of(
        st.text(min_size=1, max_size=8),
        st.integers(min_value=-10, max_value=10).filter(lambda value: value not in (0,)),
        st.booleans().filter(lambda value: value is True),
        st.floats(allow_nan=False, allow_infinity=False).filter(lambda value: value not in (0.0,)),
    )


@st.composite
def to_entries(draw: st.DrawFn) -> object:
    mapping = draw(
        st.fixed_dictionaries(
            {
                "primaryAccession": st.one_of(truthy_values(), st.none()),
                "accession": st.one_of(truthy_values(), st.none()),
                "isoformAccession": st.one_of(truthy_values(), st.none()),
                "isoform": st.one_of(truthy_values(), st.none()),
                "unexpected": nested_payloads(),
            }
        )
    )
    alternative = draw(st.one_of(truthy_values(), st.none(), nested_payloads()))
    return draw(st.one_of(st.just(mapping), st.just(alternative)))


@st.composite
def result_items(draw: st.DrawFn) -> object:
    mapping_item = {
        draw(st.sampled_from(("from", "accession", "input"))): draw(
            st.one_of(truthy_values(), st.none(), st.just(""))
        ),
        draw(st.sampled_from(("to", "mappedTo"))): draw(to_entries()),
        "noise": draw(nested_payloads()),
    }
    malformed = draw(
        st.one_of(
            st.none(),
            st.text(max_size=10),
            st.integers(-5, 5),
            st.lists(nested_payloads(), max_size=2),
        )
    )
    return draw(st.one_of(st.just(mapping_item), st.just(malformed)))


@st.composite
def result_payloads(draw: st.DrawFn) -> dict[str, object]:
    items = draw(st.lists(result_items(), max_size=4))
    container_choice = draw(st.sampled_from(("list", "mapping", "scalar")))
    if container_choice == "mapping" and items:
        container: object = {str(index): item for index, item in enumerate(items)}
    elif container_choice == "scalar" and items:
        container = items[0]
    else:
        container = items

    payload: dict[str, object] = {
        "metadata": draw(nested_payloads()),
        draw(st.sampled_from(("status", "jobStatus", "STATE", ""))): draw(nested_payloads()),
    }

    key_mode = draw(st.sampled_from(("results", "mappedTo", "both", "neither")))
    if key_mode == "results":
        payload["results"] = container
    elif key_mode == "mappedTo":
        payload["mappedTo"] = container
    elif key_mode == "both":
        payload["results"] = container
        payload["mappedTo"] = draw(st.one_of(st.just(container), nested_payloads()))

    return payload


@settings(max_examples=40, deadline=None)
@given(result_payloads())
def test_property_idmapping_results_normalizes_records(payload: dict[str, object]) -> None:
    records = parse_idmapping_results(payload)

    assert isinstance(records, list)
    for record in records:
        assert set(record) == {
            "submitted_id",
            "canonical_accession",
            "isoform_accession",
        }
        for value in record.values():
            assert value is None or isinstance(value, str)

    raw_results = payload.get("results") or payload.get("mappedTo") or []
    if isinstance(raw_results, Mapping):
        raw_iterable: Sequence[object] = [raw_results]
    elif isinstance(raw_results, Sequence):
        raw_iterable = list(raw_results)
    else:
        raw_iterable = []

    expected_max = sum(1 for item in raw_iterable if isinstance(item, Mapping))
    assert len(records) <= expected_max
