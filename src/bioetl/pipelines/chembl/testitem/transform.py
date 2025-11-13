"""Transform utilities for ChEMBL testitem pipeline array serialization and flattening."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pandas as pd

from bioetl.core.io import serialize_objects, serialize_simple_list

__all__ = [
    "serialize_simple_list",
    "serialize_objects",
    "flatten_object_col",
    "transform",
]


def flatten_object_col(
    df: pd.DataFrame, col: str, fields: Sequence[str], prefix: str
) -> pd.DataFrame:
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
            return dict.fromkeys(fields)
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
    enable_flatten = (
        getattr(cfg.transform, "enable_flatten", True) if hasattr(cfg, "transform") else True
    )
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
    if (
        enable_serialization
        and hasattr(cfg, "transform")
        and hasattr(cfg.transform, "arrays_simple_to_pipe")
    ):
        arrays_simple = cfg.transform.arrays_simple_to_pipe
        if isinstance(arrays_simple, Sequence) and not isinstance(arrays_simple, (str, bytes)):
            for col in arrays_simple:
                if col in df.columns:
                    df[col] = df[col].map(serialize_simple_list)

    # Serialize arrays of objects
    if (
        enable_serialization
        and hasattr(cfg, "transform")
        and hasattr(cfg.transform, "arrays_objects_to_header_rows")
    ):
        arrays_objects = cfg.transform.arrays_objects_to_header_rows
        if isinstance(arrays_objects, Sequence) and not isinstance(arrays_objects, (str, bytes)):
            for col in arrays_objects:
                if col in df.columns:
                    df[f"{col}__flat"] = df[col].map(serialize_objects)
                    # Remove original column after serialization
                    df = df.drop(columns=[col])

    return df
