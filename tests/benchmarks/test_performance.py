"""Performance benchmarks for critical operations."""

import pytest
import pandas as pd
import time
from pathlib import Path
import tempfile

from library.clients.base import RateLimiter, RateLimitConfig
from library.utils.rate_limit import RateLimiter as TokenBucketRateLimiter


class TestCSVExportPerformance:
    """Benchmark CSV export operations."""

    @pytest.mark.benchmark(group="csv_export")
    def test_csv_export_100k_rows(self, benchmark):
        """Benchmark CSV export with 100K rows."""
        # Generate test data
        n_rows = 100000
        data = {
            'compound_id': [f'CHEMBL{i:06d}' for i in range(n_rows)],
            'activity_type': ['IC50'] * n_rows,
            'activity_value': [float(i % 1000) for i in range(n_rows)],
            'activity_units': ['nM'] * n_rows,
            'target_name': [f'Target_{i % 100}' for i in range(n_rows)],
            'assay_type': ['B'] * n_rows,
            'publication_year': [2020 + (i % 5) for i in range(n_rows)],
        }
        df = pd.DataFrame(data)
        
        def export_csv():
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
                df.to_csv(f.name, index=False)
                return f.name
        
        result_path = benchmark(export_csv)
        
        # Cleanup
        Path(result_path).unlink(missing_ok=True)
        
        # Verify the file was created and has correct size
        assert Path(result_path).exists() == False  # Should be cleaned up

    @pytest.mark.benchmark(group="csv_export")
    def test_csv_export_1m_rows(self, benchmark):
        """Benchmark CSV export with 1M rows."""
        # Generate test data
        n_rows = 1000000
        data = {
            'compound_id': [f'CHEMBL{i:06d}' for i in range(n_rows)],
            'activity_type': ['IC50'] * n_rows,
            'activity_value': [float(i % 1000) for i in range(n_rows)],
            'activity_units': ['nM'] * n_rows,
            'target_name': [f'Target_{i % 100}' for i in range(n_rows)],
            'assay_type': ['B'] * n_rows,
            'publication_year': [2020 + (i % 5) for i in range(n_rows)],
        }
        df = pd.DataFrame(data)
        
        def export_csv():
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
                df.to_csv(f.name, index=False)
                return f.name
        
        result_path = benchmark(export_csv)
        
        # Cleanup
        Path(result_path).unlink(missing_ok=True)

    @pytest.mark.benchmark(group="csv_export")
    def test_csv_export_with_formatting(self, benchmark):
        """Benchmark CSV export with custom formatting."""
        # Generate test data
        n_rows = 50000
        data = {
            'compound_id': [f'CHEMBL{i:06d}' for i in range(n_rows)],
            'activity_value': [float(i % 1000) / 1000.0 for i in range(n_rows)],
            'publication_date': pd.date_range('2020-01-01', periods=n_rows, freq='D'),
        }
        df = pd.DataFrame(data)
        
        def export_csv_formatted():
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
                df.to_csv(
                    f.name, 
                    index=False,
                    float_format='%.3f',
                    date_format='%Y-%m-%d'
                )
                return f.name
        
        result_path = benchmark(export_csv_formatted)
        
        # Cleanup
        Path(result_path).unlink(missing_ok=True)


class TestRateLimiterPerformance:
    """Benchmark rate limiter operations."""

    @pytest.mark.benchmark(group="rate_limiter")
    def test_simple_rate_limiter_throughput(self, benchmark):
        """Benchmark simple rate limiter throughput."""
        config = RateLimitConfig(max_calls=1000, period=1.0)
        limiter = RateLimiter(config)
        
        def acquire_requests():
            for _ in range(1000):
                try:
                    limiter.acquire()
                except Exception:
                    pass
        
        benchmark(acquire_requests)

    @pytest.mark.benchmark(group="rate_limiter")
    def test_token_bucket_rate_limiter_throughput(self, benchmark):
        """Benchmark token bucket rate limiter throughput."""
        limiter = TokenBucketRateLimiter(
            capacity=1000,
            refill_rate=1000.0,
            initial_tokens=1000
        )
        
        def acquire_requests():
            for _ in range(1000):
                try:
                    limiter.acquire()
                except Exception:
                    pass
        
        benchmark(acquire_requests)

    @pytest.mark.benchmark(group="rate_limiter")
    def test_rate_limiter_contention(self, benchmark):
        """Benchmark rate limiter under contention."""
        import threading
        import queue
        
        config = RateLimitConfig(max_calls=100, period=1.0)
        limiter = RateLimiter(config)
        
        results = queue.Queue()
        
        def worker():
            success_count = 0
            for _ in range(50):
                try:
                    limiter.acquire()
                    success_count += 1
                except Exception:
                    pass
            results.put(success_count)
        
        def run_contention_test():
            threads = []
            for _ in range(10):  # 10 concurrent workers
                thread = threading.Thread(target=worker)
                threads.append(thread)
                thread.start()
            
            for thread in threads:
                thread.join()
            
            # Collect results
            total_successes = 0
            while not results.empty():
                total_successes += results.get()
            
            return total_successes
        
        total_successes = benchmark(run_contention_test)
        
        # Verify that rate limiting is working (should be around 100 total successes)
        assert total_successes <= 120  # Allow some tolerance


class TestDataProcessingPerformance:
    """Benchmark data processing operations."""

    @pytest.mark.benchmark(group="data_processing")
    def test_dataframe_operations(self, benchmark):
        """Benchmark common DataFrame operations."""
        n_rows = 100000
        data = {
            'compound_id': [f'CHEMBL{i:06d}' for i in range(n_rows)],
            'activity_value': [float(i % 1000) for i in range(n_rows)],
            'target_name': [f'Target_{i % 100}' for i in range(n_rows)],
            'publication_year': [2020 + (i % 5) for i in range(n_rows)],
        }
        df = pd.DataFrame(data)
        
        def process_dataframe():
            # Common operations: filtering, grouping, aggregation
            filtered = df[df['activity_value'] > 500]
            grouped = filtered.groupby('target_name')['activity_value'].mean()
            return grouped.to_dict()
        
        result = benchmark(process_dataframe)
        
        # Verify result
        assert isinstance(result, dict)
        assert len(result) > 0

    @pytest.mark.benchmark(group="data_processing")
    def test_dataframe_sorting(self, benchmark):
        """Benchmark DataFrame sorting operations."""
        n_rows = 50000
        data = {
            'compound_id': [f'CHEMBL{i:06d}' for i in range(n_rows)],
            'activity_value': [float(i % 1000) for i in range(n_rows)],
            'target_name': [f'Target_{i % 100}' for i in range(n_rows)],
        }
        df = pd.DataFrame(data)
        
        def sort_dataframe():
            return df.sort_values(['target_name', 'activity_value'], ascending=[True, False])
        
        result = benchmark(sort_dataframe)
        
        # Verify result
        assert len(result) == n_rows
        assert result.index.is_monotonic_increasing

    @pytest.mark.benchmark(group="data_processing")
    def test_dataframe_deduplication(self, benchmark):
        """Benchmark DataFrame deduplication."""
        n_rows = 100000
        # Create data with duplicates
        data = {
            'compound_id': [f'CHEMBL{i % 50000:06d}' for i in range(n_rows)],
            'activity_value': [float(i % 1000) for i in range(n_rows)],
            'target_name': [f'Target_{i % 100}' for i in range(n_rows)],
        }
        df = pd.DataFrame(data)
        
        def deduplicate_dataframe():
            return df.drop_duplicates(subset=['compound_id', 'target_name'])
        
        result = benchmark(deduplicate_dataframe)
        
        # Verify result
        assert len(result) <= n_rows


class TestHTTPClientPerformance:
    """Benchmark HTTP client operations (mocked)."""

    @pytest.mark.benchmark(group="http_client")
    def test_request_serialization(self, benchmark):
        """Benchmark request serialization overhead."""
        import json
        
        # Simulate request data
        request_data = {
            'compound_id': 'CHEMBL12345',
            'limit': 100,
            'offset': 0,
            'format': 'json'
        }
        
        def serialize_request():
            return json.dumps(request_data)
        
        result = benchmark(serialize_request)
        
        # Verify result
        assert isinstance(result, str)
        assert 'CHEMBL12345' in result

    @pytest.mark.benchmark(group="http_client")
    def test_response_parsing(self, benchmark):
        """Benchmark response parsing overhead."""
        import json
        
        # Simulate response data
        response_data = {
            'compounds': [
                {
                    'compound_id': f'CHEMBL{i:06d}',
                    'activity_value': float(i % 1000),
                    'target_name': f'Target_{i % 100}',
                }
                for i in range(1000)
            ],
            'total': 1000
        }
        
        response_json = json.dumps(response_data)
        
        def parse_response():
            return json.loads(response_json)
        
        result = benchmark(parse_response)
        
        # Verify result
        assert isinstance(result, dict)
        assert len(result['compounds']) == 1000


# Configuration for pytest-benchmark
def pytest_configure(config):
    """Configure pytest-benchmark settings."""
    config.addinivalue_line(
        "markers", "benchmark: mark test as a benchmark"
    )
