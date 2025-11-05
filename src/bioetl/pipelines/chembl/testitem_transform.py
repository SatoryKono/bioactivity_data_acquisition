"""Transform utilities for ChEMBL testitem pipeline array serialization and flattening."""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any, cast

import pandas as pd

from .assay_transform import escape_delims, header_rows_serialize

__all__ = [
    "serialize_simple_list",
    "serialize_objects",
    "flatten_object_col",
    "transform",
]


def serialize_simple_list(xs: Any) -> str:
    """Serialize simple list to pipe-delimited format with trailing pipe.

    Format: `a|b|c|` (trailing pipe is mandatory).

    Parameters
    ----------
    xs:
        List of values, None, or empty list.

    Returns
    -------
    str:
        Pipe-delimited string with trailing pipe, or empty string for None/empty.

    Examples
    --------
    >>> serialize_simple_list(["A01AA", "A01AB"])
    'A01AA|A01AB|'
    >>> serialize_simple_list([])
    ''
    >>> serialize_simple_list(None)
    ''
    >>> serialize_simple_list(["A|B", "C/D"])
    'A\\|B|C\\/D|'
    """
    if xs is None:
        return ""

    if not isinstance(xs, (list, tuple)):
        # Non-list value: convert to string, escape, and add trailing pipe
        return escape_delims(str(xs)) + "|"

    if not xs:
        return ""

    # Serialize each value with escaping, join with |
    escaped_values = [escape_delims("" if x is None else str(x)) for x in xs]
    return "|".join(escaped_values) + "|"


def serialize_objects(items: Any) -> str:
    """Serialize array-of-objects to header+rows format.

    Reuses header_rows_serialize from assay_transform with proper handling
    of nested structures (lists/dicts) via JSON serialization.

    Format: `header/row1/row2/...` where:
    - Header: `k1|k2|...` (ordered list of keys)
    - Row: `v1|v2|...` (values for each key, empty string if missing)
    - Nested structures are JSON-serialized and escaped

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
    >>> serialize_objects([{"xref_id": "X1", "xref_name": "N1", "xref_src": "SRC"}])
    'xref_id|xref_name|xref_src/X1|N1|SRC'
    >>> serialize_objects([{"a": "A1"}, {"a": "A2", "b": "B2"}])
    'a|b/A1|/A2|B2'
    >>> serialize_objects([{"synonyms": ["RO-64-0796"]}])
    'synonyms/["RO-64-0796"]'
    >>> serialize_objects([])
    ''
    >>> serialize_objects(None)
    ''
    """
    if items is None:
        return ""

    if not isinstance(items, list):
        # Non-list value: JSON serialize and escape
        json_str = json.dumps(items, ensure_ascii=False, sort_keys=True)
        return escape_delims(json_str)

    if not items:
        return ""

    # Type narrowing: items is now list[Any]
    typed_items: list[Any] = cast(list[Any], items)

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


def flatten_object_col(df: pd.DataFrame, col: str, fields: Sequence[str], prefix: str) -> pd.DataFrame:
    """Flatten nested object column into flat columns with prefix.

    Parameters
    ----------
    df:
        DataFrame to transform.
    col:
        Column name containing nested objects.
    fields:
        List of field names to extract from nested objects.
    prefix:
        Prefix to add to flattened column names (e.g., "molecule_hierarchy__").

    Returns
    -------
    pd.DataFrame:
        DataFrame with flattened columns added and original column removed.

    Examples
    --------
    >>> df = pd.DataFrame({
    ...     "molecule_chembl_id": ["CHEMBL1"],
    ...     "molecule_hierarchy": [{"molecule_chembl_id": "CHEMBL1", "parent_chembl_id": "CHEMBL2"}],
    ... })
    >>> result = flatten_object_col(df, "molecule_hierarchy", ["molecule_chembl_id", "parent_chembl_id"], "molecule_hierarchy__")
    >>> "molecule_hierarchy__molecule_chembl_id" in result.columns
    True
    >>> "molecule_hierarchy__parent_chembl_id" in result.columns
    True
    >>> "molecule_hierarchy" not in result.columns
    True
    """
    df = df.copy()

    if col not in df.columns:
        # Column doesn't exist: create empty columns with None
        for f in fields:
            df[f"{prefix}{f}"] = None
        return df

    def row_to_dict(obj: Any) -> dict[str, Any]:
        """Extract fields from nested object."""
        if not isinstance(obj, dict):
            return {f: None for f in fields}
        return {f: obj.get(f) for f in fields}

    # Apply transformation to extract nested fields
    expanded = df[col].map(row_to_dict).apply(pd.Series)
    expanded.columns = [f"{prefix}{c}" for c in expanded.columns]

    # Drop original column and concatenate with expanded columns
    df = df.drop(columns=[col])
    return pd.concat([df, expanded], axis=1)


def transform(df: pd.DataFrame, cfg: Any) -> pd.DataFrame:
    """Transform testitem DataFrame by flattening nested objects and serializing arrays.

    Parameters
    ----------
    df:
        DataFrame to transform.
    cfg:
        Pipeline config with transform section (enable_flatten, enable_serialization,
        arrays_simple_to_pipe, arrays_objects_to_header_rows, flatten_objects).

    Returns
    -------
    pd.DataFrame:
        Transformed DataFrame with flattened objects and serialized arrays.
    """
    df = df.copy()

    # Check if flattening is enabled
    enable_flatten = getattr(cfg.transform, "enable_flatten", True) if hasattr(cfg, "transform") else True
    enable_serialization = (
        getattr(cfg.transform, "enable_serialization", True) if hasattr(cfg, "transform") else True
    )

    # Flatten nested objects
    if enable_flatten and hasattr(cfg, "transform") and hasattr(cfg.transform, "flatten_objects"):
        flatten_objects = cfg.transform.flatten_objects
        if isinstance(flatten_objects, dict):
            for obj_col, fields in flatten_objects.items():
                if isinstance(fields, Sequence) and not isinstance(fields, (str, bytes)):
                    df = flatten_object_col(df, obj_col, fields, prefix=f"{obj_col}__")

    # Serialize simple arrays
    if enable_serialization and hasattr(cfg, "transform") and hasattr(cfg.transform, "arrays_simple_to_pipe"):
        arrays_simple = cfg.transform.arrays_simple_to_pipe
        if isinstance(arrays_simple, Sequence) and not isinstance(arrays_simple, (str, bytes)):
            for col in arrays_simple:
                if col in df.columns:
                    df[col] = df[col].map(serialize_simple_list)

    # Serialize arrays of objects
    if enable_serialization and hasattr(cfg, "transform") and hasattr(cfg.transform, "arrays_objects_to_header_rows"):
        arrays_objects = cfg.transform.arrays_objects_to_header_rows
        if isinstance(arrays_objects, Sequence) and not isinstance(arrays_objects, (str, bytes)):
            for col in arrays_objects:
                if col in df.columns:
                    df[f"{col}__flat"] = df[col].map(serialize_objects)
                    # Remove original column after serialization
                    df = df.drop(columns=[col])

    return df

