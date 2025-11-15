"""Transform utilities for ChEMBL target pipeline array serialization."""

from __future__ import annotations

import json
from collections.abc import Iterable
from typing import Any, TypeGuard, cast

import numpy as np
import numpy.typing as npt
import pandas as pd

from bioetl.core.io import header_rows_serialize
from bioetl.core.utils.iterables import is_non_string_iterable

__all__ = [
    "serialize_target_arrays",
    "extract_and_serialize_component_synonyms",
    "flatten_target_components",
]


JsonDict = dict[str, Any]


def _is_json_dict(value: Any) -> TypeGuard[JsonDict]:
    return isinstance(value, dict)


def _collect_dicts(source: Any) -> list[JsonDict]:
    """Collect dictionary entries from arbitrary source keeping order."""

    result: list[JsonDict] = []
    if _is_json_dict(source):
        result.append(source)
        return result

    if is_non_string_iterable(source):
        for element in source:
            element_any: Any = element
            if _is_json_dict(element_any):
                result.append(element_any)

    return result


def flatten_target_components(rec: dict[str, Any]) -> dict[str, Any]:
    """Flatten nested target_components data into flat columns.

    Extracts:
    - uniprot_accessions from target_components[*].accession
    - target_component_synonyms__flat from target_components[*].target_component_synonyms[*].component_synonym
    - target_components__flat (serialized container)
    - cross_references__flat (serialized from top-level)
    - component_count (counted from accessions or from top-level)

    Parameters
    ----------
    rec:
        Target record dict from ChEMBL API.

    Returns
    -------
    dict[str, Any]:
        Dictionary with flattened fields:
        - uniprot_accessions: sorted list of unique UniProt accessions (as JSON string)
        - target_component_synonyms__flat: serialized synonyms
        - target_components__flat: serialized components
        - cross_references__flat: serialized cross-references
        - component_count: count of unique accessions
    """
    result: dict[str, Any] = {
        "uniprot_accessions": "",
        "target_component_synonyms__flat": "",
        "target_components__flat": "",
        "cross_references__flat": "",
        "component_count": None,
    }

    # Extract target_components
    comps_raw: Any = rec.get("target_components") or []
    comps: list[dict[str, Any]] = _collect_dicts(comps_raw)

    # Extract UniProt accessions
    accessions: list[str] = []
    all_synonyms: list[dict[str, Any]] = []

    for component in comps:
        # Extract accession
        accession: Any = component.get("accession")
        if isinstance(accession, str) and accession.strip():
            accessions.append(accession.strip())

        # Extract synonyms from this component
        syns: Any = component.get("target_component_synonyms")
        if syns:
            all_synonyms.extend(_collect_dicts(syns))

    # Serialize uniprot_accessions as JSON array
    unique_accessions = sorted(set(accessions))
    if unique_accessions:
        result["uniprot_accessions"] = json.dumps(unique_accessions, ensure_ascii=False)
        result["component_count"] = len(unique_accessions)
    else:
        # Fallback to top-level component_count if available
        top_level_count = rec.get("component_count")
        if top_level_count is not None:
            try:
                result["component_count"] = int(top_level_count)
            except (ValueError, TypeError):
                result["component_count"] = None

    # Serialize target_component_synonyms
    if all_synonyms:
        result["target_component_synonyms__flat"] = header_rows_serialize(all_synonyms)

    # Serialize target_components
    if comps:
        result["target_components__flat"] = header_rows_serialize(comps)

    # Serialize cross_references from top-level
    xrefs_raw: Any = rec.get("cross_references") or []
    xrefs: list[dict[str, Any]] = _collect_dicts(xrefs_raw)
    if xrefs:
        result["cross_references__flat"] = header_rows_serialize(xrefs)

    return result


def extract_and_serialize_component_synonyms(target_components: Any) -> str:
    """Extract target_component_synonyms from target_components and serialize.

    Parameters
    ----------
    target_components:
        List of target component dicts, None, or empty list.

    Returns
    -------
    str:
        Serialized string in header+rows format, or empty string for None/empty.
    """
    if target_components is None:
        return ""

    components: list[dict[str, Any]] = _collect_dicts(target_components)
    if not components:
        return ""

    all_synonyms: list[dict[str, Any]] = []
    for component in components:
        syns_item: Any = component.get("target_component_synonyms")
        if syns_item:
            all_synonyms.extend(_collect_dicts(syns_item))

    if not all_synonyms:
        return ""

    return header_rows_serialize(all_synonyms)


def serialize_target_arrays(df: pd.DataFrame, config: Any) -> pd.DataFrame:
    """Serialize array fields for target pipeline.

    Uses flatten_target_components() to extract and serialize nested data
    from target_components and cross_references.

    Parameters
    ----------
    df:
        DataFrame to transform.
    config:
        Pipeline config with transform.arrays_to_header_rows.

    Returns
    -------
    pd.DataFrame:
        DataFrame with serialized array fields.
    """
    df = df.copy()

    # Get arrays to serialize from config
    arrays_to_serialize: list[str] = []
    try:
        if hasattr(config, "transform") and config.transform is not None:
            if hasattr(config.transform, "arrays_to_header_rows"):
                arrays_to_serialize = list(config.transform.arrays_to_header_rows)
    except (AttributeError, TypeError):
        pass

    # Apply flatten to each row to extract nested data
    if not df.empty:
        # Convert DataFrame rows to dicts for flatten processing
        flattened_data: list[dict[str, Any]] = []
        for _, row in df.iterrows():
            # Convert row to dict, handling NaN values
            row_dict: dict[str, Any] = row.to_dict()  # pyright: ignore[reportUnknownMemberType]
            # Replace NaN with None for proper dict handling
            for key, value in row_dict.items():
                # Handle array-like values: check if it's an array first
                if isinstance(value, np.ndarray):
                    array_value = cast(npt.NDArray[Any], value)
                    # For arrays, check if empty or if all values are NaN
                    if array_value.size == 0:
                        row_dict[key] = None
                    else:
                        # Check if all values are NaN using numpy's isnan
                        try:
                            nan_mask = np.asarray(pd.isna(array_value), dtype=bool)
                            if bool(np.all(nan_mask)):
                                row_dict[key] = None
                        except (TypeError, ValueError):
                            # If isnan fails (e.g., string array), keep as is
                            pass
                    # Otherwise keep the array as is
                elif pd.api.types.is_scalar(value):
                    try:
                        if pd.isna(value):
                            row_dict[key] = None
                    except (TypeError, ValueError):
                        # If isna fails for this type, keep as is
                        pass
                # For non-scalar, non-array values (like lists, dicts), keep as is

            # Apply flatten function
            flattened = flatten_target_components(row_dict)

            # Merge flattened fields into row dict
            row_dict.update(flattened)
            flattened_data.append(row_dict)

        # Recreate DataFrame with flattened fields
        df = pd.DataFrame(flattened_data)

    else:
        # Empty DataFrame: initialize flat columns
        df["cross_references__flat"] = ""
        df["target_components__flat"] = ""
        df["target_component_synonyms__flat"] = ""
        df["uniprot_accessions"] = ""
        df["component_count"] = None

    # Remove original array columns if they were serialized
    for col in arrays_to_serialize:
        if col in df.columns and f"{col}__flat" in df.columns:
            df = df.drop(columns=[col])

    return df
