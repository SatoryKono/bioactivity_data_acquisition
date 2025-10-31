"""Property-based tests for the ChEMBL testitem parser."""

from __future__ import annotations

import json
from typing import Any

import pytest

pytest.importorskip("hypothesis")

from hypothesis import given, settings
from hypothesis import strategies as st

from bioetl.normalizers import registry
from bioetl.sources.chembl.testitem.parser import TestItemParser
from bioetl.sources.chembl.testitem.pipeline import TestItemPipeline
from bioetl.utils.json import canonical_json


pytestmark = pytest.mark.property

_EXPECTED_COLUMNS = tuple(TestItemPipeline._expected_columns())
_JSON_FIELDS = tuple(TestItemPipeline._CHEMBL_JSON_FIELDS)
_STRUCTURE_FIELDS = tuple(TestItemPipeline._CHEMBL_STRUCTURE_FIELDS)
_FALLBACK_FIELDS = tuple(TestItemPipeline._FALLBACK_FIELDS)


def _json_scalars() -> st.SearchStrategy[Any]:
    return st.one_of(
        st.none(),
        st.booleans(),
        st.integers(),
        st.floats(allow_nan=False, allow_infinity=False),
        st.text(max_size=20),
    )


def _json_values() -> st.SearchStrategy[Any]:
    return st.recursive(
        _json_scalars(),
        lambda children: st.one_of(
            st.lists(children, max_size=5),
            st.dictionaries(st.text(min_size=1, max_size=12), children, max_size=5),
        ),
        max_leaves=20,
    )


def _synonym_entries() -> st.SearchStrategy[list[Any]]:
    text_entry = st.text(min_size=0, max_size=25)
    dict_entry = st.dictionaries(
        st.text(min_size=1, max_size=12),
        st.one_of(text_entry, st.integers(), st.none()),
        max_size=4,
    ).map(lambda data: {**data, "molecule_synonym": data.get("molecule_synonym", "")})
    return st.lists(st.one_of(text_entry, dict_entry), max_size=6)


def _structure_payload() -> st.SearchStrategy[dict[str, Any]]:
    smiles = st.text(alphabet="CNO[]=()+-#@/\\ \t", max_size=60)
    inchi = st.text(alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd", "So", "Sc")), max_size=60)
    inchi_key_chars = st.characters(whitelist_categories=("Lu", "Nd"), whitelist_characters="-")
    inchi_key = st.text(alphabet=inchi_key_chars, max_size=27)
    return st.fixed_dictionaries(
        {
            "canonical_smiles": st.one_of(smiles, st.none()),
            "standard_inchi": st.one_of(st.none(), st.just(""), st.builds(lambda core: f"InChI=1S/{core}", inchi)),
            "standard_inchi_key": st.one_of(inchi_key, st.none()),
        }
    )


def _payloads() -> st.SearchStrategy[dict[str, Any]]:
    def _properties_strategy() -> st.SearchStrategy[dict[str, Any]]:
        boolish = st.one_of(st.booleans(), st.integers(min_value=0, max_value=1), st.sampled_from(["y", "n", "yes", "no", "1", "0"]))
        numeric = st.one_of(st.integers(), st.floats(allow_infinity=False))
        base = st.dictionaries(st.text(min_size=1, max_size=12), st.one_of(boolish, numeric, st.text(max_size=20)), max_size=6)
        return base.map(lambda data: {**data})

    return st.builds(
        lambda pref_name, properties, structures, synonyms, json_fields, extras: {
            "pref_name": pref_name,
            "molecule_properties": properties,
            "molecule_structures": structures,
            "molecule_synonyms": synonyms,
            **json_fields,
            **extras,
        },
        st.one_of(st.none(), st.text(max_size=40)),
        st.one_of(st.none(), _properties_strategy()),
        st.one_of(st.none(), _structure_payload()),
        st.one_of(st.none(), _synonym_entries()),
        st.dictionaries(
            st.sampled_from(
                [
                    field
                    for field in _JSON_FIELDS
                    if field not in {"molecule_synonyms", "molecule_properties"}
                ]
            ),
            st.one_of(st.none(), _json_values()),
            max_size=max(0, len(_JSON_FIELDS) - 2),
        ),
        st.dictionaries(
            st.text(min_size=1, max_size=12).map(lambda key: f"extra_{key}"),
            st.one_of(_json_scalars(), st.text(max_size=30)),
            max_size=5,
        ),
    )


def _build_parser() -> TestItemParser:
    return TestItemParser(
        expected_columns=_EXPECTED_COLUMNS,
        property_fields=TestItemPipeline._CHEMBL_PROPERTY_FIELDS,
        structure_fields=_STRUCTURE_FIELDS,
        json_fields=_JSON_FIELDS,
        text_fields=TestItemPipeline._CHEMBL_TEXT_FIELDS,
        fallback_fields=_FALLBACK_FIELDS,
    )


@given(_payloads())
@settings(max_examples=200, deadline=None)
def test_parser_emits_all_expected_columns(payload: dict[str, Any]) -> None:
    """Every parsed record should expose the configured columns and nothing else."""

    parser = _build_parser()
    record = parser.parse(payload)

    assert set(record) == set(_EXPECTED_COLUMNS)
    for column in _FALLBACK_FIELDS:
        assert record[column] is None


@given(_payloads())
@settings(max_examples=200, deadline=None)
def test_parser_canonicalises_json_fields(payload: dict[str, Any]) -> None:
    """JSON fields must be serialised via canonical_json for determinism."""

    parser = _build_parser()
    record = parser.parse(payload)

    for field in _JSON_FIELDS:
        if field == "molecule_synonyms":
            continue
        expected = canonical_json(payload.get(field))
        assert record[field] == expected

    structures = payload.get("molecule_structures")
    expected_structures = canonical_json(structures) if structures else None
    assert record["molecule_structures"] == expected_structures


@given(_payloads())
@settings(max_examples=200, deadline=None)
def test_parser_normalises_structures_and_synonyms(payload: dict[str, Any]) -> None:
    """Normalized structures should remain valid and synonyms should be sorted."""

    parser = _build_parser()
    record = parser.parse(payload)

    smiles = record.get("standardized_smiles")
    if smiles is not None:
        assert smiles == smiles.strip()
        assert "  " not in smiles

    standard_inchi = record.get("standard_inchi")
    if standard_inchi is not None:
        assert standard_inchi.startswith("InChI=")

    inchi_key = record.get("standard_inchi_key")
    if inchi_key is not None:
        assert inchi_key == inchi_key.upper()

    serialized_synonyms = record.get("molecule_synonyms")
    if serialized_synonyms is not None:
        entries = json.loads(serialized_synonyms)
        assert entries == parser._sorted_synonym_entries(entries)
        for entry in entries:
            if isinstance(entry, dict) and "molecule_synonym" in entry:
                value = entry["molecule_synonym"]
                if isinstance(value, str):
                    assert value == value.strip()

    all_names = record.get("all_names")
    if all_names is not None:
        tokens = [token.strip() for token in all_names.split(";")]
        assert tokens == sorted({token for token in tokens if token}, key=str.lower)


@given(_payloads())
@settings(max_examples=200, deadline=None)
def test_parser_pref_name_key_is_lowercase(payload: dict[str, Any]) -> None:
    """pref_name_key should be a lower-case normalised variant of pref_name."""

    parser = _build_parser()
    record = parser.parse(payload)

    pref_name_key = record.get("pref_name_key")
    if pref_name_key is not None:
        assert pref_name_key == pref_name_key.strip()
        assert pref_name_key == pref_name_key.lower()
        expected = registry.normalize("chemistry.string", payload.get("pref_name"))
        if expected is not None:
            assert pref_name_key == expected.lower()
