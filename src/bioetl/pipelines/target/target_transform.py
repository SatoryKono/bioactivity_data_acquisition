"""Transform utilities for ChEMBL target pipeline array serialization."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, cast

import pandas as pd

from .assay_transform import header_rows_serialize

__all__ = ["serialize_target_arrays", "extract_and_serialize_component_synonyms"]


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

    if not isinstance(target_components, list):
        # Single dict: extract synonyms if present
        if isinstance(target_components, dict):
            syns = target_components.get("target_component_synonyms")
            if syns:
                return header_rows_serialize(syns if isinstance(syns, list) else [syns])
        return ""

    if not target_components:
        return ""

    # Extract all synonyms from all components
    all_synonyms: list[dict[str, Any]] = []
    for component in target_components:
        if isinstance(component, dict):
            syns = component.get("target_component_synonyms")
            if syns:
                if isinstance(syns, list):
                    all_synonyms.extend(syns)
                elif isinstance(syns, dict):
                    all_synonyms.append(cast(dict[str, Any], syns))
                else:
                    # Single value: wrap in list
                    all_synonyms.append(cast(dict[str, Any], syns))

    return header_rows_serialize(all_synonyms)


def serialize_target_arrays(df: pd.DataFrame, config: Any) -> pd.DataFrame:
    """Serialize array fields for target pipeline.

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

    # Serialize cross_references
    if "cross_references" in df.columns:
        df["cross_references__flat"] = df["cross_references"].map(header_rows_serialize)
    else:
        df["cross_references__flat"] = ""

    # Serialize target_components
    if "target_components" in df.columns:
        df["target_components__flat"] = df["target_components"].map(header_rows_serialize)
        # Extract and serialize nested target_component_synonyms
        df["target_component_synonyms__flat"] = df["target_components"].map(
            extract_and_serialize_component_synonyms
        )
    else:
        df["target_components__flat"] = ""
        df["target_component_synonyms__flat"] = ""

    # Remove original array columns if they were serialized
    for col in arrays_to_serialize:
        if col in df.columns and f"{col}__flat" in df.columns:
            df = df.drop(columns=[col])

    return df

