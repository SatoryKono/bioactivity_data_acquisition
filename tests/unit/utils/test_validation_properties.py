"""Property-based tests for validation helpers."""

from __future__ import annotations

from collections import Counter
from typing import Any

import pandas as pd
import pytest

pytest.importorskip("hypothesis")

from hypothesis import given, settings
from hypothesis import strategies as st

from bioetl.utils.validation import summarize_schema_errors


@st.composite
def failure_cases_dataframes(draw) -> pd.DataFrame:
    """Generate Pandera-like ``failure_cases`` frames with diverse contents."""

    row_count = draw(st.integers(min_value=0, max_value=8))
    include_column = draw(st.booleans())
    include_check = draw(st.booleans())
    include_failure = draw(st.booleans())
    extra_names = draw(
        st.lists(
            st.text(min_size=1, max_size=10).filter(
                lambda name: name not in {"column", "check", "failure_case"}
            ),
            max_size=3,
            unique=True,
        )
    )

    base_value_strategy = st.one_of(
        st.text(max_size=10),
        st.integers(),
        st.floats(allow_nan=True, allow_infinity=False),
        st.booleans(),
        st.none(),
    )
    column_value_strategy = st.one_of(
        st.text(max_size=10),
        st.integers(),
        st.floats(allow_nan=True, allow_infinity=False),
        st.none(),
    )
    failure_value_strategy = st.one_of(
        st.text(max_size=12),
        st.integers(),
        st.floats(allow_nan=True, allow_infinity=False),
        st.none(),
    )

    def draw_series(strategy: st.SearchStrategy[Any]) -> list[Any]:
        return draw(st.lists(strategy, min_size=row_count, max_size=row_count))

    data: dict[str, list[Any]] = {}

    if include_column:
        data["column"] = draw_series(column_value_strategy)
    if include_check:
        data["check"] = draw_series(base_value_strategy)
    if include_failure:
        data["failure_case"] = draw_series(failure_value_strategy)

    for name in extra_names:
        data[name] = draw_series(base_value_strategy)

    if not data:
        # Ensure the DataFrame has at least one column even when all flags are False.
        data["fallback"] = draw_series(base_value_strategy)

    return pd.DataFrame(data)


failure_cases_inputs = st.one_of(st.none(), failure_cases_dataframes())


@given(failure_cases_inputs)
@settings(max_examples=200, deadline=None)
def testsummarize_schema_errors_properties(failure_cases: pd.DataFrame | None) -> None:
    """The helper always produces stable, well-formed issue payloads."""

    result = summarize_schema_errors(failure_cases)

    assert isinstance(result, list)
    assert all(isinstance(item, dict) for item in result)

    if not isinstance(failure_cases, pd.DataFrame) or failure_cases.empty:
        assert result == []
        return

    working = failure_cases.copy()
    column_missing = "column" not in working.columns
    if column_missing:
        working["column"] = None

    grouped = list(working.groupby("column", dropna=False))
    assert len(result) == len(grouped)

    expected_entries: list[dict[str, Any]] = []
    per_column_expected = Counter()

    for column_value, group in grouped:
        column_name = "<dataframe>" if pd.isna(column_value) else str(column_value)
        per_column_expected[column_name] += int(group.shape[0])

        if "check" in group.columns:
            check_values = group["check"].dropna().astype(str).unique().tolist()
            check_value = ", ".join(sorted(check_values)) if check_values else "<unspecified>"
        else:
            check_value = "<unspecified>"

        if "failure_case" in group.columns:
            failure_values = group["failure_case"].dropna().astype(str).unique().tolist()
            details_value = ", ".join(failure_values[:5])
        else:
            failure_values = []
            details_value = ""

        expected_entries.append(
            {
                "column": column_name,
                "check": check_value,
                "count": int(group.shape[0]),
                "details": details_value,
                "unique_failures": failure_values[:5],
            }
        )

    per_column_result = Counter()
    unmatched_expected = expected_entries.copy()

    for issue in result:
        assert issue["issue_type"] == "schema"
        assert issue["severity"] == "error"
        assert isinstance(issue["column"], str)
        assert isinstance(issue["check"], str)
        assert isinstance(issue["details"], str)
        assert isinstance(issue["count"], int)
        assert issue["count"] >= 0

        per_column_result[issue["column"]] += issue["count"]

        match_index = next(
            (
                idx
                for idx, expected in enumerate(unmatched_expected)
                if expected["column"] == issue["column"]
                and expected["check"] == issue["check"]
                and expected["count"] == issue["count"]
                and expected["details"] == issue["details"]
            ),
            None,
        )
        assert match_index is not None

        matched = unmatched_expected.pop(match_index)

        if issue["details"]:
            detail_parts = issue["details"].split(", ")
            assert len(detail_parts) == len(set(detail_parts))
            assert len(detail_parts) <= 5
            assert detail_parts == matched["unique_failures"]
        else:
            assert matched["unique_failures"] == [] or matched["unique_failures"] == [""]

    assert not unmatched_expected
    assert per_column_result == per_column_expected

    if column_missing:
        assert set(issue["column"] for issue in result) == {"<dataframe>"}


def testsummarize_schema_errors_missing_column_defaults() -> None:
    """When the ``column`` field is absent every issue targets the DataFrame sentinel."""

    failure_cases = pd.DataFrame(
        {
            "check": ["length_check"],
            "failure_case": ["missing value"],
        }
    )

    result = summarize_schema_errors(failure_cases)

    assert result == [
        {
            "issue_type": "schema",
            "severity": "error",
            "column": "<dataframe>",
            "check": "length_check",
            "count": 1,
            "details": "missing value",
        }
    ]
