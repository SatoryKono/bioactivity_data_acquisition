"""Reusable validation helpers for schema modules."""

from __future__ import annotations

import json
import math
from collections.abc import Iterable, Mapping, Sequence
from numbers import Number

import pandas as pd

from bioetl.schemas.common_schema import RELATION_SYMBOLS

__all__ = [
    "RELATIONS",
    "is_json_string",
    "validate_json_series",
    "is_activity_property_item",
    "validate_activity_properties",
    "validate_membership_series",
    "validate_relation_series",
]

RELATIONS: tuple[str, ...] = RELATION_SYMBOLS


def is_json_string(value: object, *, allow_empty: bool = False) -> bool:
    """Return True when value is a JSON-encoded string."""

    if value is None or value is pd.NA:
        return False
    if not isinstance(value, str):
        return False
    if not allow_empty and not value.strip():
        return False
    try:
        json.loads(value)
    except (TypeError, ValueError):
        return False
    return True


def validate_json_series(
    series: pd.Series,
    *,
    allow_empty: bool = False,
    optional: bool = False,
) -> bool:
    """Vectorized validator ensuring entries store JSON strings.

    When ``optional`` is set to ``True`` the validator ignores missing values
    and accepts empty columns.
    """

    candidate = series.dropna() if optional else series
    if optional and candidate.empty:
        return True
    return bool(candidate.map(lambda value: is_json_string(value, allow_empty=allow_empty)).all())


def is_activity_property_item(
    item: Mapping[str, object],
    *,
    allowed_keys: Iterable[str],
) -> bool:
    """Return True if the mapping uses the allowed keys and value types."""

    expected_keys = set(allowed_keys)
    if set(item.keys()) != expected_keys:
        return False

    type_value = item["type"]
    if type_value is not None and not isinstance(type_value, str):
        return False

    relation_value = item["relation"]
    if relation_value is not None and not isinstance(relation_value, str):
        return False

    units_value = item["units"]
    if units_value is not None and not isinstance(units_value, str):
        return False

    numeric_value = item["value"]
    if numeric_value is not None and not isinstance(numeric_value, (Number, str)):
        return False

    text_value = item["text_value"]
    if text_value is not None and not isinstance(text_value, str):
        return False

    result_flag = item["result_flag"]
    if result_flag is not None and not isinstance(result_flag, bool):
        if isinstance(result_flag, int):
            if result_flag not in (0, 1):
                return False
        else:
            return False

    return True


def validate_activity_properties(
    value: object,
    *,
    allowed_keys: Iterable[str],
) -> bool:
    """Element-wise validator ensuring activity_properties stores normalized JSON arrays."""

    if value is None:
        return True
    if value is pd.NA:
        return True
    if isinstance(value, (float, int)) and not isinstance(value, bool):
        try:
            if math.isnan(float(value)):
                return True
        except (TypeError, ValueError):
            pass
    if not isinstance(value, str):
        return False

    try:
        payload = json.loads(value)
    except (TypeError, ValueError):
        return False

    if isinstance(payload, list):
        candidate_items = payload
    elif isinstance(payload, Mapping):
        candidate_items = list(payload.values())
    else:
        return False

    for item in candidate_items:
        if not isinstance(item, Mapping):
            return False
        if not is_activity_property_item(dict(item), allowed_keys=allowed_keys):
            return False

    return True


def validate_membership_series(
    series: pd.Series,
    *,
    allowed: Iterable[str],
) -> bool:
    """Ensure every non-null entry belongs to the allowed value set."""

    allowed_set = frozenset(str(item).strip() for item in allowed if str(item).strip())
    if not allowed_set:
        return True
    non_null = series.dropna()
    if non_null.empty:
        return True
    normalized = non_null.astype(str).str.strip()
    return bool(normalized.isin(allowed_set).all())


def validate_relation_series(
    series: pd.Series,
    *,
    allowed: Sequence[str] | None = None,
) -> bool:
    """Specialized validator for relation fields supporting custom domain overrides."""

    domain = allowed if allowed is not None else RELATIONS
    return validate_membership_series(series, allowed=domain)


