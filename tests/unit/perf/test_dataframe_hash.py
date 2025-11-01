"""Performance benchmarks for dataframe hashing helpers."""

from __future__ import annotations

from collections.abc import Iterable

import numpy as np
import pandas as pd
import pytest

from bioetl.core.hashing import generate_hash_row
from bioetl.utils.dataframe import finalize_pipeline_output


@pytest.fixture(scope="module")
def _sample_dataframe() -> pd.DataFrame:
    """Build a representative dataframe with mixed dtypes for benchmarking."""

    size = 2_000
    base_ts = pd.Timestamp("2024-01-01T00:00:00+00:00")
    data = {
        "business_id": [f"CHEMBL{i:07d}" for i in range(size)],
        "value": np.linspace(0.0, 1.0, num=size, dtype=float),
        "created_at": pd.date_range(base_ts, periods=size, freq="min", tz="UTC"),
        "flag": np.tile([True, False], size // 2),
    }
    return pd.DataFrame(data)


@pytest.mark.benchmark(group="hash_row")
def test_finalize_pipeline_output_hashing_benchmark(
    benchmark: pytest.BenchmarkFixture,
    _sample_dataframe: pd.DataFrame,
) -> None:
    """Benchmark the vectorised finalize_pipeline_output hashing implementation."""

    def _run() -> pd.DataFrame:
        return finalize_pipeline_output(
            _sample_dataframe,
            business_key="business_id",
            sort_by=["business_id"],
            ascending=[True],
            pipeline_version="bench-1.0",
            source_system="chembl",
            chembl_release="ChEMBL_BENCH",
            extracted_at="2024-01-01T00:00:00+00:00",
            run_id="benchmark-run",
        )

    result = benchmark(_run)
    assert "hash_row" in result.columns
    assert result["hash_row"].str.len().eq(64).all()


def _hash_rows_with_apply(df: pd.DataFrame) -> Iterable[str]:
    """Baseline implementation that mimics the previous row-wise apply approach."""

    return df.apply(lambda row: generate_hash_row(row.to_dict()), axis=1)


@pytest.mark.benchmark(group="hash_row")
def test_hash_row_apply_baseline(
    benchmark: pytest.BenchmarkFixture,
    _sample_dataframe: pd.DataFrame,
) -> None:
    """Measure the legacy apply-based row hashing for comparison."""

    def _run() -> Iterable[str]:
        return _hash_rows_with_apply(_sample_dataframe)

    baseline = benchmark(_run)
    assert len(baseline) == len(_sample_dataframe)
