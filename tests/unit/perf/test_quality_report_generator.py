"""Benchmarks and regression tests for :class:`QualityReportGenerator`."""

from __future__ import annotations

import time
from collections.abc import Callable

import numpy as np
import pandas as pd
import pandas.testing as pdt
import pytest

from bioetl.core.output_writer import QualityReportGenerator


def _legacy_quality_report(
    df: pd.DataFrame,
    issues: list[dict[str, object]] | None = None,
    qc_metrics: dict[str, object] | None = None,
) -> pd.DataFrame:
    """Replicate the previous column-wise implementation for comparison."""

    rows: list[dict[str, object]] = []

    for column in df.columns:
        null_count = df[column].isna().sum()
        null_fraction = null_count / len(df) if len(df) > 0 else 0
        unique_count = df[column].nunique()

        rows.append(
            {
                "metric": "column_profile",
                "column": column,
                "null_count": null_count,
                "null_fraction": null_fraction,
                "unique_count": unique_count,
                "dtype": str(df[column].dtype),
            }
        )

    if issues:
        for issue in issues:
            record = {"metric": issue.get("metric", "validation_issue")}
            record.update(issue)
            rows.append(record)

    if qc_metrics:
        for name, value in qc_metrics.items():
            entry: dict[str, object] = {
                "metric": "qc_metric",
                "name": name,
                "value": value,
            }
            rows.append(entry)

    return pd.DataFrame(rows)


@pytest.fixture(scope="module")
def _wide_dataframe() -> pd.DataFrame:
    """Construct a wide, mixed-type dataframe representative of QC outputs."""

    rng = np.random.default_rng(2024)
    row_count = 800
    float_columns = 120
    int_columns = 60
    bool_columns = 10
    string_columns = 10

    data = {
        f"float_{idx:03d}": rng.normal(loc=0.0, scale=1.0, size=row_count)
        for idx in range(float_columns)
    }
    data.update(
        {
            f"int_{idx:03d}": rng.integers(0, 500, size=row_count)
            for idx in range(int_columns)
        }
    )

    df = pd.DataFrame(data)
    mask = rng.random(df.shape) < 0.05
    df = df.mask(mask)

    for idx in range(bool_columns):
        values = pd.Series(rng.random(row_count) < 0.5, dtype="boolean")
        null_mask = rng.random(row_count) < 0.05
        values.loc[null_mask] = pd.NA
        df[f"bool_{idx:03d}"] = values

    categories = np.array(list("ABCDE"))
    for idx in range(string_columns):
        values = pd.Series(rng.choice(categories, size=row_count), dtype="string")
        null_mask = rng.random(row_count) < 0.1
        values.loc[null_mask] = pd.NA
        df[f"str_{idx:03d}"] = values

    return df


def test_quality_report_generator_matches_legacy(_wide_dataframe: pd.DataFrame) -> None:
    """Ensure the optimised implementation produces identical QC outputs."""

    generator = QualityReportGenerator()
    issues = [
        {
            "metric": "validation_issue",
            "column": "float_000",
            "severity": "warning",
            "message": "Example warning",
        }
    ]
    qc_metrics = {"row_count": len(_wide_dataframe), "duplicate_rows": 0}

    expected = _legacy_quality_report(_wide_dataframe, issues=issues, qc_metrics=qc_metrics)
    result = generator.generate(_wide_dataframe, issues=issues, qc_metrics=qc_metrics)

    pdt.assert_frame_equal(result, expected)


def _measure_runtime(func: Callable[[], pd.DataFrame], repeat: int = 5) -> float:
    """Measure mean runtime of a callable using a small number of repeats."""

    # Warm-up to reduce one-off allocation effects.
    func()
    durations: list[float] = []
    for _ in range(repeat):
        start = time.perf_counter()
        func()
        durations.append(time.perf_counter() - start)
    return sum(durations) / len(durations)


def test_quality_report_generator_wide_dataframe_performance(
    _wide_dataframe: pd.DataFrame,
) -> None:
    """The vectorised generator should outperform the legacy column-wise version."""

    generator = QualityReportGenerator()

    new_runtime = _measure_runtime(lambda: generator.generate(_wide_dataframe))
    legacy_runtime = _measure_runtime(lambda: _legacy_quality_report(_wide_dataframe))

    # Require a noticeable improvement to safeguard against regressions.
    assert new_runtime <= legacy_runtime * 0.9
