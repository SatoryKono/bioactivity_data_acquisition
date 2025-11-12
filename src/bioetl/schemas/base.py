"""Base utilities for creating Pandera schemas."""

from __future__ import annotations

from collections.abc import Sequence

from pandera import Check, Column, DataFrameSchema


def create_schema(
    *,
    columns: dict[str, Column],
    version: str,
    name: str,
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
    return DataFrameSchema(
        columns,
        ordered=ordered,
        coerce=False,  # Disable coercion at schema level - types are normalized in transform
        strict=strict,
        checks=list(checks) if checks else None,
        name=f"{name}_v{version}",
    )


__all__ = ["create_schema"]
