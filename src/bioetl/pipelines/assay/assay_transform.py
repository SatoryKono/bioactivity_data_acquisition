"""Transform utilities for ChEMBL assay pipeline array serialization."""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any, cast

import pandas as pd

__all__ = ["header_rows_serialize", "serialize_array_fields"]


def escape_delims(s: str) -> str:
    r"""Escape pipe and slash delimiters in string values.

    Parameters
    ----------
    s:
        Input string to escape.

    Returns
    -------
    str:
        String with escaped delimiters: `|` → `\|`, `/` → `\/`, `\` → `\\`.
    """
    return s.replace("\\", "\\\\").replace("|", "\\|").replace("/", "\\/")


def header_rows_serialize(items: Any) -> str:
    """Serialize array-of-objects to header+rows format.

    Format: `header/row1/row2/...` where:
    - Header: `k1|k2|...` (ordered list of keys)
    - Row: `v1|v2|...` (values for each key, empty string if missing)

    Parameters
    ----------
    items:
        List of dicts, None, or empty list.

    Returns
    -------
    str:
        Serialized string in header+rows format, or empty string for None/empty.

    Examples
    --------
    >>> header_rows_serialize([{"a": "A", "b": "B"}])
    'a|b/A|B'
    >>> header_rows_serialize([{"a": "A1"}, {"a": "A2", "b": "B2"}])
    'a|b/A1|/A2|B2'
    >>> header_rows_serialize([])
    ''
    >>> header_rows_serialize(None)
    ''
    >>> header_rows_serialize([{"x": "A|B", "y": "C/D"}])
    'x|y/A\\|B|C\\/D'
    """
    if items is None:
        return ""

    if not isinstance(items, list):
        # Non-list value: JSON serialize and escape delimiters
        json_str = json.dumps(items, ensure_ascii=False, sort_keys=True)
        return escape_delims(json_str)

    # Type narrowing: items is now list[Any]
    typed_items: list[Any] = cast(list[Any], items)

    if not typed_items:
        return ""

    # Gather keys deterministically:
    # 1. Preserve order from first item
    # 2. Append unseen keys from other items in alphabetical order
    ordered_keys: list[str] = []
    seen_set: set[str] = set()

    # First pass: collect keys from first item in order
    if len(typed_items) > 0 and isinstance(typed_items[0], dict):
        first_item: dict[str, Any] = cast(dict[str, Any], typed_items[0])
        for key in first_item.keys():
            if key not in seen_set:
                ordered_keys.append(key)
                seen_set.add(key)

    # Second pass: collect remaining keys from other items, then sort alphabetically
    remaining_keys: set[str] = set()
    for item in typed_items[1:]:
        if isinstance(item, dict):
            remaining_item: dict[str, Any] = cast(dict[str, Any], item)
            for key in remaining_item.keys():
                if key not in seen_set:
                    remaining_keys.add(key)
                    seen_set.add(key)

    # Append remaining keys in alphabetical order
    ordered_keys.extend(sorted(remaining_keys))

    # Build header
    header = "|".join(ordered_keys)

    # Build rows
    rows: list[str] = []
    for item in typed_items:
        if not isinstance(item, dict):
            # Fallback: JSON serialize non-dict item
            json_str = json.dumps(item, ensure_ascii=False, sort_keys=True)
            rows.append(escape_delims(json_str))
            continue

        # Extract values for each key
        item_dict: dict[str, Any] = cast(dict[str, Any], item)
        values: list[str] = []
        for key in ordered_keys:
            value: Any | None = item_dict.get(key)
            if value is None:
                values.append("")
            elif isinstance(value, (list, dict)):
                # Nested structure: JSON serialize and escape
                json_str = json.dumps(value, ensure_ascii=False, sort_keys=True)
                values.append(escape_delims(json_str))
            else:
                # Scalar value: convert to string and escape
                values.append(escape_delims(str(value)))

        rows.append("|".join(values))

    # Join header and rows
    if not rows:
        return ""

    return header + "/" + "/".join(rows)


def serialize_array_fields(df: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    """Serialize array-of-object fields to header+rows format.

    Parameters
    ----------
    df:
        DataFrame to transform.
    columns:
        List of column names to serialize.

    Returns
    -------
    pd.DataFrame:
        DataFrame with specified columns serialized to strings.
    """
    df = df.copy()

    for col in columns:
        if col in df.columns:
            df[col] = df[col].map(header_rows_serialize)

    return df

