"""Canonical mappers for ChEMBL activity records."""

from __future__ import annotations

import json
import math
import re
from typing import Any, Mapping

import pandas as pd

RELATION_NORMALIZATION = {
    "≤": "<=",
    "≦": "<=",
    "⩽": "<=",
    "≥": ">=",
    "≧": ">=",
    "⩾": ">=",
}

RELATION_WHITELIST = {"=", ">", "<", ">=", "<=", "~"}

UNIT_SYNONYMS = {
    "um": "µM",
    "μm": "µM",
    "µm": "µM",
    "uM": "µM",
    "UM": "µM",
    "micromolar": "µM",
    "millimolar": "mM",
    "MM": "mM",
    "mm": "mM",
    "NM": "nM",
    "nm": "nM",
    "nanomolar": "nM",
}

BOOLEAN_TRUE_VALUES = {"true", "1", "yes", "y", True, 1}
BOOLEAN_FALSE_VALUES = {"false", "0", "no", "n", False, 0}

FLOAT_KEYS = ("bei", "sei", "le", "lle")
PROPERTY_VALUE_KEYS = ("type", "name", "description", "value", "units", "relation")

DEFAULT_STANDARD_RELATION = "="
DEFAULT_PUBLISHED_RELATION = "="


def _canonicalize_whitespace(value: str) -> str:
    value = re.sub(r"\s+", " ", value.strip())
    return value


def _normalize_string(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        if value.strip() == "":
            return ""
        return _canonicalize_whitespace(value)
    return _canonicalize_whitespace(str(value))


def _normalize_identifier(value: Any) -> str:
    normalized = _normalize_string(value)
    return normalized.upper()


def _normalize_bao(value: Any) -> str:
    normalized = _normalize_identifier(value)
    if normalized and not normalized.startswith("BAO_"):
        return ""
    return normalized


def _normalize_units(value: Any) -> str:
    normalized = _normalize_string(value).replace("μ", "µ")
    if not normalized:
        return ""
    key = normalized.lower()
    if key in UNIT_SYNONYMS:
        return UNIT_SYNONYMS[key]
    return normalized


def _normalize_relation(value: Any, *, default: str) -> str:
    relation = _normalize_string(value)
    if not relation:
        relation = default
    relation = RELATION_NORMALIZATION.get(relation, relation)
    if relation not in RELATION_WHITELIST:
        return default
    return relation


def _normalize_float(value: Any) -> float | pd.NA:
    if value is None or (isinstance(value, str) and value.strip() == ""):
        return pd.NA
    try:
        return float(value)
    except (TypeError, ValueError):
        return pd.NA


def _normalize_int(value: Any) -> int | pd.NA:
    if value is None or (isinstance(value, str) and value.strip() == ""):
        return pd.NA
    try:
        return int(value)
    except (TypeError, ValueError):
        return pd.NA


def _normalize_bool(value: Any, *, default: bool = False) -> bool:
    if value in BOOLEAN_TRUE_VALUES:
        return True
    if value in BOOLEAN_FALSE_VALUES:
        return False
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in BOOLEAN_TRUE_VALUES:
            return True
        if lowered in BOOLEAN_FALSE_VALUES:
            return False
    return default


def _canonicalize_float_str(value: float | pd.NA) -> str:
    if value is None or pd.isna(value):
        return ""
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return ""
    if math.isnan(numeric):
        return ""
    return f"{numeric:.6f}"


def _canonicalize_mapping(mapping: Mapping[str, Any]) -> dict[str, Any]:
    canonical: dict[str, Any] = {}
    for key, value in mapping.items():
        if value is None:
            canonical[key] = None
            continue
        if isinstance(value, str):
            canonical[key] = _normalize_string(value)
        elif isinstance(value, (int, float)):
            canonical[key] = float(value)
        elif isinstance(value, bool):
            canonical[key] = bool(value)
        else:
            canonical[key] = value
    return canonical


def _canonicalize_json(mapping: Mapping[str, Any]) -> str:
    canonical = {}
    for key, value in mapping.items():
        if isinstance(value, float):
            canonical[key] = float(f"{value:.6f}")
        elif isinstance(value, (int, bool)) or value is None:
            canonical[key] = value
        elif isinstance(value, str):
            canonical[key] = _normalize_string(value)
        else:
            canonical[key] = value
    return json.dumps(canonical, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def normalize_ligand_efficiency(payload: Any) -> tuple[str, dict[str, float | pd.NA]]:
    canonical_payload: Mapping[str, Any] | None
    if isinstance(payload, str):
        try:
            parsed = json.loads(payload)
            canonical_payload = parsed if isinstance(parsed, Mapping) else None
        except json.JSONDecodeError:
            canonical_payload = None
    elif isinstance(payload, Mapping):
        canonical_payload = payload
    else:
        canonical_payload = None

    metrics: dict[str, float | pd.NA] = {key: pd.NA for key in FLOAT_KEYS}
    if canonical_payload is None:
        return "", metrics

    normalized_map = _canonicalize_mapping(canonical_payload)
    has_numeric = False
    for key in FLOAT_KEYS:
        value = normalized_map.get(key) or normalized_map.get(key.upper())
        metrics[key] = _normalize_float(value)
        if not pd.isna(metrics[key]):
            has_numeric = True

    if not has_numeric:
        return "", metrics

    return _canonicalize_json({k.upper(): metrics[k] if not pd.isna(metrics[k]) else None for k in FLOAT_KEYS}), metrics


def normalize_activity_properties(payload: Any) -> str:
    """Canonicalize activity properties preserving all fields."""
    properties: list[Mapping[str, Any]] = []
    if isinstance(payload, str):
        try:
            decoded = json.loads(payload)
        except json.JSONDecodeError:
            decoded = None
        if isinstance(decoded, list):
            properties = [item for item in decoded if isinstance(item, Mapping)]
        elif isinstance(decoded, Mapping):
            properties = [decoded]
    elif isinstance(payload, list):
        properties = [item for item in payload if isinstance(item, Mapping)]
    elif isinstance(payload, Mapping):
        properties = [payload]

    if not properties:
        return ""

    canonical_rows: list[str] = []
    for prop in properties:
        normalized = _canonicalize_mapping(prop)
        parts: list[str] = []
        keys = [key for key in PROPERTY_VALUE_KEYS if key in normalized]
        extra_keys = sorted({key for key in normalized if key not in PROPERTY_VALUE_KEYS})
        for key in keys + extra_keys:
            value = normalized.get(key)
            if value is None:
                formatted = ""
            elif isinstance(value, bool):
                formatted = "true" if value else "false"
            elif isinstance(value, (int, float)):
                formatted = _canonicalize_float_str(float(value))
            elif key == "relation":
                formatted = _normalize_relation(value, default="=")
            else:
                formatted = _normalize_string(str(value))
            parts.append(f"{key}={formatted}")
        canonical_rows.append("|".join(parts))

    canonical_rows = sorted(set(filter(None, canonical_rows)))
    return "\n".join(canonical_rows)


def build_compound_key(row: Mapping[str, Any]) -> str:
    molecule = _normalize_identifier(row.get("molecule_chembl_id"))
    standard_type = _normalize_string(row.get("standard_type"))
    target = _normalize_identifier(row.get("target_chembl_id"))
    return "|".join([molecule, standard_type, target]).strip("|")


def map_activity_row(row: Mapping[str, Any]) -> dict[str, Any]:
    """Map raw activity payload to canonical schema fields."""
    activity_id = _normalize_int(row.get("activity_id") or row.get("activity_chembl_id"))

    molecule_id = _normalize_identifier(row.get("molecule_chembl_id"))
    assay_id = _normalize_identifier(row.get("assay_chembl_id"))
    target_id = _normalize_identifier(row.get("target_chembl_id"))
    document_id = _normalize_identifier(row.get("document_chembl_id"))

    published_relation = _normalize_relation(row.get("published_relation"), default=DEFAULT_PUBLISHED_RELATION)
    standard_relation = _normalize_relation(row.get("standard_relation"), default=DEFAULT_STANDARD_RELATION)

    ligand_payload = row.get("ligand_efficiency")
    if ligand_payload is None:
        ligand_payload = row.get("ligand_eff")
    ligand_efficiency, ligand_metrics = normalize_ligand_efficiency(ligand_payload)

    is_citation_raw = row.get("is_citation")
    if is_citation_raw is None:
        is_citation_value = document_id != ""
    else:
        is_citation_value = is_citation_raw

    data = {
        "activity_id": activity_id,
        "molecule_chembl_id": molecule_id,
        "assay_chembl_id": assay_id,
        "target_chembl_id": target_id,
        "document_chembl_id": document_id,
        "published_type": _normalize_string(row.get("published_type")),
        "published_relation": published_relation,
        "published_value": _normalize_float(row.get("published_value")),
        "published_units": _normalize_units(row.get("published_units")),
        "standard_type": _normalize_string(row.get("standard_type")),
        "standard_relation": standard_relation,
        "standard_value": _normalize_float(row.get("standard_value")),
        "standard_units": _normalize_units(row.get("standard_units")),
        "standard_flag": _normalize_int(row.get("standard_flag")),
        "lower_bound": _normalize_float(row.get("lower_bound")),
        "upper_bound": _normalize_float(row.get("upper_bound")),
        "is_censored": _normalize_bool(row.get("is_censored")),
        "pchembl_value": _normalize_float(row.get("pchembl_value")),
        "activity_comment": _normalize_string(row.get("activity_comment")),
        "data_validity_comment": _normalize_string(row.get("data_validity_comment")),
        "bao_endpoint": _normalize_bao(row.get("bao_endpoint")),
        "bao_format": _normalize_bao(row.get("bao_format")),
        "bao_label": _normalize_string(row.get("bao_label")),
        "canonical_smiles": _normalize_string(row.get("canonical_smiles")),
        "target_organism": _normalize_string(row.get("target_organism")).title(),
        "target_tax_id": _normalize_int(row.get("target_tax_id")),
        "action_type": _normalize_string(row.get("action_type")),
        "potential_duplicate": _normalize_int(row.get("potential_duplicate")),
        "uo_units": _normalize_identifier(row.get("uo_units")),
        "qudt_units": _normalize_string(row.get("qudt_units")),
        "src_id": _normalize_int(row.get("src_id")),
        "ligand_efficiency": ligand_efficiency,
        "activity_properties": normalize_activity_properties(row.get("activity_properties") or row.get("activity_properties_json")),
        "compound_key": _normalize_string(row.get("compound_key")) or build_compound_key(row),
        "is_citation": _normalize_bool(is_citation_value),
        "high_citation_rate": _normalize_bool(row.get("high_citation_rate")),
        "exact_data_citation": _normalize_bool(row.get("exact_data_citation")),
        "rounded_data_citation": _normalize_bool(row.get("rounded_data_citation")),
    }

    data.update(ligand_metrics)
    return data
