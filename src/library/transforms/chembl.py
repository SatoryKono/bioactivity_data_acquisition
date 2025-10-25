"""ChEMBL-specific data transformations."""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Iterable
from typing import Any

logger = logging.getLogger(__name__)

_EC_TOKEN_SPLIT = re.compile(r"[|;,/\\\s]+")
_EC_FULL_PATTERN = re.compile(r"^\d+(?:\.(?:\d+|-)){3}$")


def extract_target_payload(data: Any) -> dict[str, Any]:
    """Return a target record from ``data`` regardless of envelope shape."""

    if isinstance(data, dict):
        if "target" in data:
            items = data["target"]
            if isinstance(items, list):
                return items[0] if items else {}
            if isinstance(items, dict):
                return items
        if "targets" in data:
            items = data["targets"]
            if isinstance(items, list):
                return items[0] if items else {}
            if isinstance(items, dict):
                return items
        return data
    if isinstance(data, list):
        return data[0] if data else {}
    return {}


def _get_items(container: Any, key: str) -> list[Any]:
    """Return a list of items from a container that may be a dict or list."""

    if isinstance(container, dict):
        items = container.get(key, [])
    else:
        items = container or []
    if isinstance(items, dict):
        return [items]
    if isinstance(items, list):
        return items
    return []


def _parse_gene_synonyms(synonyms: list[dict[str, str]]) -> str:
    """Return pipe-delimited gene symbols from ``synonyms``."""

    names = {s["component_synonym"] for s in synonyms if s.get("syn_type") in {"GENE_SYMBOL", "GENE_SYMBOL_OTHER"}}
    return "|".join(sorted(names))


def _parse_ec_codes(synonyms: list[dict[str, str]]) -> str:
    """Return pipe-delimited EC codes from ``synonyms``."""

    codes = {s["component_synonym"] for s in synonyms if s.get("syn_type") == "EC_NUMBER"}
    return "|".join(sorted(codes))


def _parse_alt_names(synonyms: list[dict[str, str]]) -> str:
    """Return pipe-delimited alternative names from ``synonyms``."""

    names = {s["component_synonym"] for s in synonyms if s.get("syn_type") == "UNIPROT"}
    return "|".join(sorted(names))


def _parse_uniprot_id(xrefs: list[dict[str, str]], target_chembl_id: str) -> tuple[str, str]:
    """Return (uniprot_id, mapping_uniprot_id) from ``xrefs``."""

    uniprot_id = ""
    mapping_uniprot_id = ""
    for xref in xrefs:
        if xref.get("xref_src") == "UniProt":
            uniprot_id = xref.get("xref_id", "")
            mapping_uniprot_id = uniprot_id
            break
    return uniprot_id, mapping_uniprot_id


def _parse_hgnc(xrefs: list[dict[str, str]]) -> tuple[str, str]:
    """Return (hgnc_name, hgnc_id) from ``xrefs``."""

    hgnc_name = ""
    hgnc_id = ""
    for xref in xrefs:
        if xref.get("xref_src") == "HGNC":
            hgnc_id = xref.get("xref_id", "")
            hgnc_name = xref.get("xref_name", "")
            break
    return hgnc_name, hgnc_id


def _stringify(value: Any) -> str:
    """Convert ``value`` to string, handling None and empty values."""

    if value is None:
        return ""
    return str(value)


def _serialize_structure(data: Any) -> str:
    """Serialize nested structure to JSON string with deterministic ordering."""

    if data is None:
        return ""
    return json.dumps(data, sort_keys=True, separators=(",", ":"))


def parse_target_record(data: dict[str, Any]) -> dict[str, Any]:
    """Transform a raw target record into a flat dictionary."""

    components = _get_items(data.get("target_components"), "target_component")
    if not components:
        logger.debug("No components found in target record: %s", data)
        components = []
    comp = components[0] if components else {}
    synonyms = _get_items(comp.get("target_component_synonyms"), "target_component_synonym")
    xrefs = _get_items(comp.get("target_component_xrefs"), "target")

    gene_syn = _parse_gene_synonyms(synonyms)
    ec_code = _parse_ec_codes(synonyms)
    alt_name = _parse_alt_names(synonyms)
    uniprot_id, mapping_uniprot_id = _parse_uniprot_id(xrefs, data.get("target_chembl_id", ""))
    hgnc_name, hgnc_id = _parse_hgnc(xrefs)

    # Define target fields
    target_fields = [
        "pref_name",
        "target_chembl_id",
        "component_description",
        "component_id",
        "relationship",
        "gene",
        "uniprot_id",
        "mapping_uniprot_id",
        "chembl_alternative_name",
        "ec_code",
        "hgnc_name",
        "hgnc_id",
        "target_type",
        "tax_id",
        "species_group_flag",
        "target_components",
        "protein_classifications",
        "cross_references",
        "reaction_ec_numbers",
    ]

    # Initialize result with empty values
    res = {field: "" for field in target_fields}

    # Update with actual data
    res.update(
        {
            "pref_name": data.get("pref_name", ""),
            "target_chembl_id": data.get("target_chembl_id", ""),
            "component_description": comp.get("component_description", ""),
            "component_id": str(comp.get("component_id", "")),
            "relationship": data.get("target_type", ""),
            "gene": gene_syn,
            "uniprot_id": uniprot_id,
            "mapping_uniprot_id": mapping_uniprot_id,
            "chembl_alternative_name": alt_name,
            "ec_code": ec_code,
            "hgnc_name": hgnc_name,
            "hgnc_id": hgnc_id,
            "target_type": _stringify(data.get("target_type")),
            "tax_id": _stringify(data.get("tax_id")),
            "species_group_flag": _stringify(data.get("species_group_flag")),
            "target_components": _serialize_structure(components),
            "protein_classifications": _serialize_structure(data.get("protein_classifications")),
            "cross_references": _serialize_structure(data.get("cross_references")),
            "reaction_ec_numbers": _collect_reaction_ec_numbers(components),
        }
    )
    return res


def _collect_reaction_ec_numbers(components: list[dict[str, Any]]) -> str:
    """Collect EC numbers from target components."""

    REACTION_EC_EXCLUDED_XREFS = {
        "REACTOME",
        "RHEA",
        "METACYC",
        "EC_REACTION",
    }

    candidates: list[str] = []
    for comp in components:
        xrefs = comp.get("target_component_xrefs", {})
        if isinstance(xrefs, dict):
            xref_list = xrefs.get("target", [])
        else:
            xref_list = xrefs or []
        for xref in xref_list:
            if isinstance(xref, dict):
                src = xref.get("xref_src", "")
                if src not in REACTION_EC_EXCLUDED_XREFS:
                    value = xref.get("xref_id", "")
                    if isinstance(value, str) and value:
                        candidates.append(value)
    return normalize_reaction_ec_numbers(candidates)


def normalize_reaction_ec_numbers(values: Iterable[str | None]) -> str:
    """Return a pipe-delimited string of sanitized EC numbers from ``values``."""

    numbers = _collect_normalized_ec_tokens(values)
    return "|".join(sorted(numbers))


def _collect_normalized_ec_tokens(values: Iterable[str | None]) -> set[str]:
    """Return a set of EC numbers extracted from ``values``."""

    numbers: set[str] = set()
    for value in values:
        if not isinstance(value, str) or not value:
            continue
        for token in _split_ec_candidates(value):
            cleaned = _normalise_ec_token(token)
            if cleaned and _EC_FULL_PATTERN.fullmatch(cleaned):
                numbers.add(cleaned)
    return numbers


def _split_ec_candidates(value: str) -> list[str]:
    """Return tokens parsed from ``value`` using standard separators."""

    return [token for token in _EC_TOKEN_SPLIT.split(value) if token]


def _normalise_ec_token(token: str) -> str:
    """Return a cleaned EC token stripped of prefixes and whitespace."""

    token = token.strip()
    if not token:
        return ""
    upper = token.upper()
    if upper.startswith("EC"):
        token = token[2:]
        token = token.lstrip(":._- ")
    return token.strip()
