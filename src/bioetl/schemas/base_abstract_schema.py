"""Base utilities for creating Pandera schemas."""

from __future__ import annotations

from collections.abc import Sequence

from pandera import Check, Column, DataFrameSchema


def create_schema(
    *,
    columns: dict[str, Column],
    version: str,
    name: str,
    column_order: Sequence[str] | None = None,
    strict: bool = False,
    ordered: bool = True,
    checks: Sequence[Check] | None = None,
) -> DataFrameSchema:
    """Create a standardized DataFrameSchema with common settings.

    Parameters
    ----------
    columns
        Dictionary of column name to Column definition.
    version
        Schema version string (e.g., "1.0.0").
    name
        Schema name (e.g., "AssaySchema").
    strict
        Whether to enforce strict schema validation (only allow defined columns).
    ordered
        Whether to enforce stable column ordering during validation.
    checks
        Optional sequence of dataframe-level checks to apply.

    Returns
    -------
    DataFrameSchema
        A configured DataFrameSchema with ordered=True and coerce=False.
    """
    schema_columns = dict(columns)
    if column_order:
        normalized_order = list(column_order)
        missing_columns = [column for column in normalized_order if column not in schema_columns]
        if missing_columns:
            msg = f"column_order references missing columns: {missing_columns}"
            raise ValueError(msg)
        duplicate_columns = {column for column in normalized_order if normalized_order.count(column) > 1}
        if duplicate_columns:
            msg = f"column_order contains duplicates: {sorted(duplicate_columns)}"
            raise ValueError(msg)
        ordered_columns: dict[str, Column] = {
            column: schema_columns[column] for column in normalized_order
        }
        for column_name, column_schema in schema_columns.items():
            if column_name not in ordered_columns:
                ordered_columns[column_name] = column_schema
        schema_columns = ordered_columns

    metadata = {
        "name": name,
        "version": version,
        "column_order": tuple(column_order) if column_order else None,
    }

    return DataFrameSchema(
        schema_columns,
        ordered=ordered,
        coerce=False,  # Disable coercion at schema level - types are normalized in transform
        strict=strict,
        checks=list(checks) if checks else None,
        name=f"{name}_v{version}",
        metadata=metadata,
    )


__all__ = ["create_schema"]
