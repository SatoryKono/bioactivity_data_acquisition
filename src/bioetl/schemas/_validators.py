"""Reusable validation helpers for schema modules."""

from __future__ import annotations

import json
import math
from collections.abc import Iterable, Mapping
from numbers import Number

import pandas as pd

__all__ = [
    "is_json_string",
    "validate_json_series",
    "validate_optional_json_series",
    "is_activity_property_item",
    "validate_activity_properties",
]


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


def validate_json_series(series: pd.Series, *, allow_empty: bool = False) -> bool:
    """Vectorized validator ensuring every entry stores a JSON string."""

    return bool(series.map(lambda value: is_json_string(value, allow_empty=allow_empty)).all())


def validate_optional_json_series(series: pd.Series, *, allow_empty: bool = False) -> bool:
    """Vectorized validator ensuring optional JSON columns contain valid payloads when present."""

    non_null = series.dropna()
    if non_null.empty:
        return True
    return bool(
        non_null.map(lambda value: is_json_string(value, allow_empty=allow_empty)).all()
    )


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


