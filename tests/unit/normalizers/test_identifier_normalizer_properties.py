"""Property-based tests for the identifier normalizer."""

from __future__ import annotations

import re

from hypothesis import given
from hypothesis import strategies as st

from bioetl.normalizers.identifier import IdentifierNormalizer

normalizer = IdentifierNormalizer()


_WHITESPACE_CHARS = [" ", "\t", "\n", "\r", "\f", "\v"]
_UNICODE_NOISE = st.text(
    alphabet=st.characters(blacklist_categories=("Cs",)),
    min_size=0,
    max_size=4,
)
_NONEMPTY_UNICODE_NOISE = st.text(
    alphabet=st.characters(blacklist_categories=("Cs",)),
    min_size=1,
    max_size=4,
)
_WHITESPACE = st.text(alphabet=_WHITESPACE_CHARS, min_size=0, max_size=3)


_CANONICAL_IDENTIFIERS = [
    "10.1206/S1234-5678",  # DOI
    "12345678",  # PMID
    "CHEMBL1993",
    "Q9H2X6",  # UniProt accession
    "BSYNRYMUTXBXSQ-UHFFFAOYSA-N",  # InChI Key
]


@st.composite
def identifier_inputs(draw: st.DrawFn) -> str:
    """Generate messy identifier strings covering diverse edge cases."""

    base = draw(st.sampled_from(_CANONICAL_IDENTIFIERS))
    toggles = draw(st.lists(st.booleans(), min_size=len(base), max_size=len(base)))
    variant_chars: list[str] = []
    for char, use_upper in zip(base, toggles):
        if char.isalpha():
            variant_chars.append(char.upper() if use_upper else char.lower())
        else:
            variant_chars.append(char)
    variant = "".join(variant_chars)

    leading_ws = draw(_WHITESPACE)
    trailing_ws = draw(_WHITESPACE)
    prefix_noise = draw(st.one_of(st.just(""), _UNICODE_NOISE))
    suffix_noise = draw(st.one_of(st.just(""), _UNICODE_NOISE))

    # Optionally embed separators or Unicode noise around the identifier.
    separator = draw(st.one_of(st.just(""), st.sampled_from(["|", "::", "—", "✶"])) )
    combined = f"{prefix_noise}{leading_ws}{separator}{variant}{separator}{trailing_ws}{suffix_noise}"
    return combined


@given(identifier_inputs())
def test_identifier_normalize_is_idempotent_and_validates(value: str) -> None:
    """Normalization should be stable and compatible with validation."""

    once = normalizer.normalize(value)
    if once is not None:
        twice = normalizer.normalize(once)
        assert twice == once
        assert normalizer.validate(once)
    else:
        assert once is None


_VALID_ORCIDS = ["0000-0002-1825-0097", "1234-5678-9012-345X"]
_ORCID_PREFIXES = ["", "https://orcid.org/", "http://orcid.org/"]


@st.composite
def orcid_inputs(draw: st.DrawFn) -> tuple[str, str | None]:
    base = draw(st.sampled_from(_VALID_ORCIDS))
    is_valid = draw(st.booleans())

    toggles = draw(st.lists(st.booleans(), min_size=len(base), max_size=len(base)))
    varied = "".join(
        char.upper() if use_upper else char.lower() if char.isalpha() else char
        for char, use_upper in zip(base, toggles)
    )

    prefix = draw(st.sampled_from(_ORCID_PREFIXES))
    leading_ws = draw(_WHITESPACE)
    trailing_ws = draw(_WHITESPACE)

    if is_valid:
        raw_core = varied
        expected = base.upper()
        suffix_noise = ""
    else:
        # Introduce corruption via noise, missing separators, or extra characters.
        mutation = draw(
            st.one_of(
                st.just(varied.replace("-", "")),
                st.just(varied[:-1]),
                st.just(varied + "Z"),
                st.builds(lambda n: varied + n, _NONEMPTY_UNICODE_NOISE),
            )
        )
        raw_core = mutation
        expected = None
        suffix_noise = draw(_UNICODE_NOISE)

    raw = f"{leading_ws}{prefix}{raw_core}{suffix_noise}{trailing_ws}"
    return raw, expected


@given(orcid_inputs())
def test_normalize_orcid_strips_prefix_and_handles_noise(data: tuple[str, str | None]) -> None:
    raw, expected = data

    result = normalizer.normalize_orcid(raw)
    assert result == expected
    if result is not None:
        assert re.fullmatch(normalizer.PATTERNS["orcid"], result)
        assert result == result.upper()


_VALID_OPENALEX_IDS = ["W1234567890", "A42", "S987654321"]


@st.composite
def openalex_inputs(draw: st.DrawFn) -> tuple[str, str | None]:
    base = draw(st.sampled_from(_VALID_OPENALEX_IDS))
    is_valid = draw(st.booleans())

    prefix = draw(st.sampled_from(["", "https://openalex.org/"]))
    leading_ws = draw(_WHITESPACE)
    trailing_ws = draw(_WHITESPACE)

    if is_valid:
        raw_core = base
        expected = base
        suffix_noise = ""
    else:
        mutation = draw(
            st.one_of(
                st.just(base.lower()),
                st.just(base[0].lower() + base[1:]),
                st.just(base[0] + "-" + base[1:]),
                st.builds(lambda starter: f"{starter}{base}", st.sampled_from(["!", "✶", "数据", "λ", "_", "¿"])),
            )
        )
        raw_core = mutation
        expected = None
        suffix_noise = draw(_UNICODE_NOISE)

    raw = f"{leading_ws}{prefix}{raw_core}{suffix_noise}{trailing_ws}"
    return raw, expected


@given(openalex_inputs())
def test_normalize_openalex_handles_prefix_and_rejects_bad_inputs(data: tuple[str, str | None]) -> None:
    raw, expected = data

    result = normalizer.normalize_openalex_id(raw)
    assert result == expected
    if result is not None:
        assert re.fullmatch(normalizer.PATTERNS["openalex"], result)
