"""Helpers for working with schema validation payloads."""

from __future__ import annotations

from typing import Any

import pandas as pd


def _summarize_schema_errors(failure_cases: pd.DataFrame | None) -> list[dict[str, Any]]:
    """Convert Pandera ``failure_cases`` frames into structured issue payloads."""

    issues: list[dict[str, Any]] = []
    if not isinstance(failure_cases, pd.DataFrame) or failure_cases.empty:
        return issues

    working = failure_cases.copy()
    if "column" not in working.columns:
        working["column"] = None

    for column, group in working.groupby("column", dropna=False):
        if pd.isna(column):
            column_name = "<dataframe>"
        else:
            column_name = str(column)

        if "check" in group.columns:
            check_values = group["check"].dropna().astype(str).unique().tolist()
        else:
            check_values = []

        if "failure_case" in group.columns:
            failure_values = group["failure_case"].dropna().astype(str).unique().tolist()
        else:
            failure_values = []

        issues.append(
            {
                "issue_type": "schema",
                "severity": "error",
                "column": column_name,
                "check": ", ".join(sorted(check_values)) if check_values else "<unspecified>",
                "count": int(group.shape[0]),
                "details": ", ".join(failure_values[:5]),
            }
        )

    return issues


__all__ = ["_summarize_schema_errors"]

