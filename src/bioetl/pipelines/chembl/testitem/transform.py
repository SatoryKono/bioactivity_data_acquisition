"""Transform utilities for ChEMBL testitem pipeline array serialization and flattening."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import pandas as pd

from bioetl.core.io import serialize_objects, serialize_simple_list

__all__ = [
    "serialize_simple_list",
    "serialize_objects",
    "flatten_object_col",
    "transform",
]

_DEFAULT_FLATTEN_OBJECTS: dict[str, tuple[str, ...]] = {
    "molecule_hierarchy": ("molecule_chembl_id", "parent_chembl_id"),
    "molecule_properties": (
        "alogp",
        "aromatic_rings",
        "cx_logd",
        "cx_logp",
        "cx_most_apka",
        "cx_most_bpka",
        "full_molformula",
        "full_mwt",
        "hba",
        "hba_lipinski",
        "hbd",
        "hbd_lipinski",
        "heavy_atoms",
        "molecular_species",
        "mw_freebase",
        "mw_monoisotopic",
        "num_lipinski_ro5_violations",
        "num_ro5_violations",
        "psa",
        "qed_weighted",
        "ro3_pass",
        "rtb",
    ),
    "molecule_structures": (
        "canonical_smiles",
        "molfile",
        "standard_inchi",
        "standard_inchi_key",
    ),
}
_DEFAULT_ARRAYS_SIMPLE: tuple[str, ...] = ("atc_classifications",)
_DEFAULT_ARRAYS_OBJECTS: tuple[str, ...] = ("cross_references", "molecule_synonyms")


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
    expanded.columns = pd.Index([f"{prefix}{c}" for c in expanded.columns])

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

    transform_cfg = getattr(cfg, "transform", None)
    if transform_cfg is None and hasattr(cfg, "domain"):
        transform_cfg = getattr(cfg.domain, "transform", None)

    # Check if flattening is enabled
    enable_flatten = getattr(transform_cfg, "enable_flatten", True) if transform_cfg else True
    enable_serialization = (
        getattr(transform_cfg, "enable_serialization", True) if transform_cfg else True
    )

    # Flatten nested objects
    flatten_objects: Mapping[str, Sequence[str]] | None = None
    if transform_cfg and hasattr(transform_cfg, "flatten_objects"):
        candidate = getattr(transform_cfg, "flatten_objects")
        if isinstance(candidate, Mapping):
            normalized: dict[str, Sequence[str]] = {}
            for key, value in candidate.items():
                if isinstance(key, str) and isinstance(value, Sequence) and not isinstance(
                    value, (str, bytes)
                ):
                    normalized[key] = tuple(value)
            flatten_objects = normalized or None

    if enable_flatten:
        objects_to_flatten = flatten_objects or _DEFAULT_FLATTEN_OBJECTS
        for obj_col, fields in objects_to_flatten.items():
            if isinstance(fields, Sequence) and not isinstance(fields, (str, bytes)):
                df = flatten_object_col(df, obj_col, fields, prefix=f"{obj_col}__")

    # Serialize simple arrays
    arrays_simple: Sequence[str] = ()
    if transform_cfg and hasattr(transform_cfg, "arrays_simple_to_pipe"):
        candidate = getattr(transform_cfg, "arrays_simple_to_pipe")
        if isinstance(candidate, Sequence) and not isinstance(candidate, (str, bytes)):
            arrays_simple = candidate
    if enable_serialization:
        simple_targets = arrays_simple or _DEFAULT_ARRAYS_SIMPLE
        for col in simple_targets:
            if col in df.columns:
                df[col] = df[col].map(serialize_simple_list)

    # Serialize arrays of objects
    arrays_objects: Sequence[str] = ()
    if transform_cfg and hasattr(transform_cfg, "arrays_objects_to_header_rows"):
        candidate = getattr(transform_cfg, "arrays_objects_to_header_rows")
        if isinstance(candidate, Sequence) and not isinstance(candidate, (str, bytes)):
            arrays_objects = candidate

    if enable_serialization:
        object_targets = arrays_objects or _DEFAULT_ARRAYS_OBJECTS
        for col in object_targets:
            if col in df.columns:
                df[f"{col}__flat"] = df[col].map(serialize_objects)
                df = df.drop(columns=[col])

    return df
