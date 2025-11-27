"""Tests for ChemblBatchExecutor batch processing utilities."""

from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest

from bioetl.core.http import CircuitBreakerOpenError
from bioetl.core.pipeline.unified import BatchExtractionStats
from bioetl.pipelines.chembl.batch_executor import (
    ChemblBatchExecutor,
    execute_chembl_batches,
)


class TestChemblBatchExecutor:
    """Test suite for ChemblBatchExecutor."""

    def test_init_default_batch_size(self) -> None:
        """Test default batch size initialization."""
        executor = ChemblBatchExecutor()
        assert executor.batch_size == 25

    def test_init_custom_batch_size(self) -> None:
        """Test custom batch size initialization."""
        executor = ChemblBatchExecutor(batch_size=10)
        assert executor.batch_size == 10

    def test_init_batch_size_sanitization(self) -> None:
        """Test batch size sanitization in __post_init__."""
        # Test None
        executor = ChemblBatchExecutor(batch_size=None)
        assert executor.batch_size == 25

        # Test negative
        executor = ChemblBatchExecutor(batch_size=-5)
        assert executor.batch_size == 1

        # Test zero
        executor = ChemblBatchExecutor(batch_size=0)
        assert executor.batch_size == 1

        # Test too large
        executor = ChemblBatchExecutor(batch_size=100)
        assert executor.batch_size == 25

    def test_build_batches_with_ids(self) -> None:
        """Test batch building with IDs."""
        executor = ChemblBatchExecutor(batch_size=3)
        ids = ["a", "b", "c", "d", "e", "f", "g"]
        batches = executor._build_batches(ids)

        assert len(batches) == 3
        assert batches[0] == ["a", "b", "c"]
        assert batches[1] == ["d", "e", "f"]
        assert batches[2] == ["g"]

    def test_build_batches_without_ids(self) -> None:
        """Test batch building without IDs."""
        executor = ChemblBatchExecutor()
        batches = executor._build_batches(None)

        assert len(batches) == 1
        assert batches[0] is None

    def test_build_batches_empty_list(self) -> None:
        """Test batch building with empty list."""
        executor = ChemblBatchExecutor()
        batches = executor._build_batches([])

        assert len(batches) == 1
        assert batches[0] is None

    def test_sanitize_batch_size_static(self) -> None:
        """Test static batch size sanitization."""
        assert ChemblBatchExecutor._sanitize_batch_size(None) == 25
        assert ChemblBatchExecutor._sanitize_batch_size(10) == 10
        assert ChemblBatchExecutor._sanitize_batch_size(0) == 1
        assert ChemblBatchExecutor._sanitize_batch_size(-5) == 1
        assert ChemblBatchExecutor._sanitize_batch_size(100) == 25

    def test_run_successful_batches(self) -> None:
        """Test successful batch execution."""
        executor = ChemblBatchExecutor(batch_size=2)

        def mock_fetcher(batch):
            if batch == ["a", "b"]:
                return [{"id": "a"}, {"id": "b"}], {"api_calls": 1}
            elif batch == ["c"]:
                return [{"id": "c"}], {"api_calls": 1}
            return []

        df, stats = executor.run(mock_fetcher, ["a", "b", "c"])

        assert df.shape[0] == 3
        assert list(df["id"]) == ["a", "b", "c"]
        assert stats.rows == 3
        assert stats.api_calls == 2
        assert stats.success_count == 3
        assert stats.fallback_count == 0
        assert stats.error_count == 0
        assert stats.duration_seconds > 0

    def test_run_with_dataframe_result(self) -> None:
        """Test batch execution with DataFrame result."""
        executor = ChemblBatchExecutor()

        def mock_fetcher(batch):
            return pd.DataFrame([{"id": "x"}, {"id": "y"}]), {"cache_hit": True}

        df, stats = executor.run(mock_fetcher, ["x", "y"])

        assert df.shape[0] == 2
        assert list(df["id"]) == ["x", "y"]
        assert stats.api_calls == 0  # cache hit
        assert stats.cache_hits == 2

    def test_run_with_mixed_result_types(self) -> None:
        """Test batch execution with mixed result types."""
        executor = ChemblBatchExecutor(batch_size=1)

        def mock_fetcher(batch):
            if batch == ["a"]:
                return [{"id": "a"}]  # List of dicts
            elif batch == ["b"]:
                return pd.DataFrame([{"id": "b"}])  # DataFrame
            return []

        df, stats = executor.run(mock_fetcher, ["a", "b"])

        assert df.shape[0] == 2
        assert list(df["id"]) == ["a", "b"]

    def test_run_with_fallback_metadata(self) -> None:
        """Test batch execution with fallback metadata."""
        executor = ChemblBatchExecutor()

        def mock_fetcher(batch):
            return [{"id": "x"}], {"fallback": 2, "api_calls": 1}

        df, stats = executor.run(mock_fetcher, ["x"])

        assert stats.fallback_count == 2
        assert stats.api_calls == 1

    def test_run_circuit_breaker_stops_execution(self) -> None:
        """Test that circuit breaker stops batch execution."""
        executor = ChemblBatchExecutor(batch_size=2)
        call_count = 0

        def mock_fetcher(batch):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return [{"id": "a"}], {"api_calls": 1}
            raise CircuitBreakerOpenError("circuit open")

        df, stats = executor.run(mock_fetcher, ["a", "b", "c", "d"])

        assert df.shape[0] == 1
        assert list(df["id"]) == ["a"]
        assert stats.error_count == 1
        assert stats.rows == 1

    def test_run_general_exception_continues(self) -> None:
        """Test that general exceptions don't stop execution."""
        executor = ChemblBatchExecutor(batch_size=2)

        def mock_fetcher(batch):
            if batch == ["a", "b"]:
                return [{"id": "a"}], {"api_calls": 1}
            elif batch == ["c", "d"]:
                raise ValueError("some error")
            return []

        df, stats = executor.run(mock_fetcher, ["a", "b", "c", "d"])

        assert df.shape[0] == 1
        assert list(df["id"]) == ["a"]
        assert stats.error_count == 1
        assert stats.rows == 1

    def test_run_empty_result(self) -> None:
        """Test batch execution with empty result."""
        executor = ChemblBatchExecutor()

        def mock_fetcher(batch):
            return [], {"api_calls": 0}

        df, stats = executor.run(mock_fetcher, ["x", "y"])

        assert df.empty
        assert stats.rows == 0
        assert stats.api_calls == 0

    def test_run_no_ids(self) -> None:
        """Test batch execution with no IDs."""
        executor = ChemblBatchExecutor()

        def mock_fetcher(batch):
            assert batch is None
            return [], {"api_calls": 0}

        df, stats = executor.run(mock_fetcher, None)

        assert df.empty
        assert stats.rows == 0

    def test_run_metadata_defaults(self) -> None:
        """Test batch execution with missing metadata."""
        executor = ChemblBatchExecutor()

        def mock_fetcher(batch):
            return [{"id": "x"}]  # No metadata tuple

        df, stats = executor.run(mock_fetcher, ["x"])

        assert stats.api_calls == 1  # defaults to 1 when no cache_hit
        assert stats.cache_hits == 0
        assert stats.fallback_count == 0


def test_execute_chembl_batches_function() -> None:
    """Test the execute_chembl_batches convenience function."""
    def mock_fetcher(batch):
        return [{"id": "x"}], {"api_calls": 1}

    df, stats = execute_chembl_batches(mock_fetcher, ["x"], batch_size=5)

    assert df.shape[0] == 1
    assert stats.api_calls == 1


def test_execute_chembl_batches_default_batch_size() -> None:
    """Test execute_chembl_batches with default batch size."""
    def mock_fetcher(batch):
        return [{"id": "x"}], {"api_calls": 1}

    df, stats = execute_chembl_batches(mock_fetcher, ["x"])

    assert df.shape[0] == 1
    assert stats.api_calls == 1
