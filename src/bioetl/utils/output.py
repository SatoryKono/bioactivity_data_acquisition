"""Utilities for normalising final pipeline datasets."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import pandas as pd

from bioetl.schemas.base import BaseSchema

from .dataframe import finalize_pipeline_output

DEFAULT_METADATA_OVERWRITE: tuple[str, ...] = ("pipeline_version", "extracted_at")


def _prepare_metadata(
    df: pd.DataFrame,
    metadata: Mapping[str, Any] | None,
    *,
    overwrite: Sequence[str] | None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Apply metadata defaults prior to finalisation.

    Parameters
    ----------
    df:
        Source dataframe that will be copied before applying mutations.
    metadata:
        Mapping of column name to value that should be populated before the
        deterministic ordering stage runs. Values may be scalars or series; in
        both cases the caller is responsible for ensuring they align with the
        dataframe index.
    overwrite:
        Column names that must be overwritten with the provided value instead of
        filling missing entries only.

    Returns
    -------
    tuple[pd.DataFrame, dict[str, Any]]
        A copy of the dataframe with metadata applied where appropriate and the
        remaining metadata values (after extracting the canonical fields) that
        should be forwarded to :func:`finalize_pipeline_output`.
    """

    if metadata is None:
        return df.copy(), {}

    overwrite_set = set(str(column) for column in (overwrite or ()))
    working = df.copy()
    metadata_values = dict(metadata)

    def apply(column: str, value: Any) -> None:
        if value is None:
            return

        if column in overwrite_set:
            working[column] = value
        elif column in working.columns:
            working[column] = working[column].fillna(value)
        else:
            working[column] = value

    # Extract canonical metadata fields but still apply them locally so that the
    # caller can control how values are propagated (fill vs overwrite).
    pipeline_version = metadata_values.pop("pipeline_version", None)
    source_system = metadata_values.pop("source_system", None)
    chembl_release = metadata_values.pop("chembl_release", None)
    extracted_at = metadata_values.pop("extracted_at", None)

    apply("pipeline_version", pipeline_version)
    apply("source_system", source_system)
    apply("chembl_release", chembl_release)
    apply("extracted_at", extracted_at)

    for column, value in metadata_values.items():
        apply(str(column), value)

    forwarded: dict[str, Any] = {
        "pipeline_version": pipeline_version,
        "source_system": source_system if "source_system" in overwrite_set else None,
        "chembl_release": chembl_release if "chembl_release" in overwrite_set else None,
        "extracted_at": extracted_at,
    }

    return working, forwarded


def finalize_output_dataset(
    df: pd.DataFrame,
    *,
    business_key: str,
    sort_by: Sequence[str] | None = None,
    ascending: Sequence[bool] | bool | None = None,
    schema: type[BaseSchema] | None = None,
    metadata: Mapping[str, Any] | None = None,
    overwrite_metadata: Sequence[str] | None = DEFAULT_METADATA_OVERWRITE,
) -> pd.DataFrame:
    """Apply deterministic metadata, hashing and ordering to pipeline datasets.

    The helper wraps :func:`bioetl.utils.dataframe.finalize_pipeline_output`
    while providing a consistent interface for populating standard metadata
    columns (``pipeline_version``, ``source_system``, ``chembl_release`` and
    ``extracted_at``).  Pipelines can also pass arbitrary metadata values that
    should be materialised alongside the canonical columns.
    """

    if df.empty:
        # Preserve existing schema expectations for empty datasets by returning
        # a shallow copy. Consumers historically relied on ``transform`` being a
        # no-op for empty frames.
        return df.copy()

    prepared, forwarded_metadata = _prepare_metadata(
        df,
        metadata,
        overwrite=overwrite_metadata,
    )

    return finalize_pipeline_output(
        prepared,
        business_key=business_key,
        sort_by=sort_by,
        ascending=ascending,
        pipeline_version=forwarded_metadata.get("pipeline_version"),
        source_system=forwarded_metadata.get("source_system"),
        chembl_release=forwarded_metadata.get("chembl_release"),
        extracted_at=forwarded_metadata.get("extracted_at"),
        schema=schema,
    )


__all__ = ["finalize_output_dataset"]

