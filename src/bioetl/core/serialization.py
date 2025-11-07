"""Common serialization helpers for deterministic pipeline outputs."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, TypeAlias, cast

import pandas as pd
from pandas.api.types import is_scalar

__all__ = [
    "escape_delims",
    "header_rows_serialize",
    "serialize_array_fields",
    "serialize_simple_list",
    "serialize_objects",
]


ScalarValue: TypeAlias = str | int | float | bool
SerializableSimpleList: TypeAlias = Iterable[Any] | Mapping[str, Any] | ScalarValue | None


def escape_delims(value: str) -> str:
    """Escape pipe and slash delimiters for deterministic string payloads."""

    return value.replace("\\", "\\\\").replace("|", "\\|").replace("/", "\\/")


def header_rows_serialize(items: Any) -> str:
    """Serialize an array of objects into canonical header/rows form."""

    if items is None:
        return ""

    if is_scalar(items) and pd.isna(items):
        return ""

    if not isinstance(items, list):
        json_str = json.dumps(items, ensure_ascii=False, sort_keys=True)
        return escape_delims(json_str)

    typed_items: list[Any] = list(cast(Iterable[Any], items))
    if not typed_items:
        return ""

    ordered_keys: list[str] = []
    seen_set: set[str] = set()

    first_item = typed_items[0]
    if isinstance(first_item, Mapping):
        first_mapping = cast(Mapping[str, Any], first_item)
        for raw_key in first_mapping.keys():
            key = str(raw_key)
            if key not in seen_set:
                ordered_keys.append(key)
                seen_set.add(key)

    remaining_keys: set[str] = set()
    for item in typed_items[1:]:
        if isinstance(item, Mapping):
            mapping_item = cast(Mapping[str, Any], item)
            for raw_key in mapping_item.keys():
                key = str(raw_key)
                if key not in seen_set:
                    remaining_keys.add(key)
                    seen_set.add(key)

    ordered_keys.extend(sorted(remaining_keys))

    header = "|".join(ordered_keys)

    rows: list[str] = []
    for item in typed_items:
        if not isinstance(item, Mapping):
            json_str = json.dumps(item, ensure_ascii=False, sort_keys=True)
            rows.append(escape_delims(json_str))
            continue

        mapping_item = cast(Mapping[str, Any], item)
        row_values: list[str] = []
        for key in ordered_keys:
            value = mapping_item.get(key)
            if value is None:
                row_values.append("")
            elif isinstance(value, (list, dict)):
                json_str = json.dumps(value, ensure_ascii=False, sort_keys=True)
                row_values.append(escape_delims(json_str))
            else:
                row_values.append(escape_delims(str(value)))

        rows.append("|".join(row_values))

    if not rows:
        return ""

    return header + "/" + "/".join(rows)


def serialize_array_fields(df: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    """Serialize array-of-object columns of ``df`` using ``header_rows_serialize``."""

    df_result = df.copy()
    for column in columns:
        if column in df_result.columns:
            null_mask = df_result[column].isna()
            serialized = df_result[column].map(header_rows_serialize).astype("string")
            df_result[column] = serialized
            df_result.loc[null_mask, column] = pd.NA
    return df_result


def serialize_simple_list(values: SerializableSimpleList) -> str:
    """Serialize simple values or iterables to pipe-delimited string with trailing pipe."""

    if values is None:
        return ""

    if not isinstance(values, (list, tuple)):
        return escape_delims(str(values)) + "|"

    if not values:
        return ""

    escaped = [escape_delims("" if value is None else str(value)) for value in values]
    return "|".join(escaped) + "|"


def serialize_objects(items: Any) -> str:
    """Serialize arbitrary objects (preferably list[dict]) via header/rows format."""

    return header_rows_serialize(items)
