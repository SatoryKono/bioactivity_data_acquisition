# Testing Standards

This document defines the testing standards for the `bioetl` project. All tests
**MUST** follow these standards to ensure reliability, reproducibility, and
maintainability.

## Principles

- **No Network Calls**: Unit tests **MUST NOT** make network requests; use mocks
  or fixtures.
- **Golden Tests**: Critical outputs **MUST** have golden tests for
  deterministic verification.
- **Property-Based Tests**: Critical transformations **SHOULD** use
  property-based testing (Hypothesis).
- **Coverage Threshold**: Minimum coverage **MUST** be 85% (enforced in CI).

## Test Categories and Markers

Tests **MUST** use pytest markers to categorize test types:

- `@pytest.mark.unit`: Unit tests (isolated, fast)
- `@pytest.mark.integration`: Integration tests (multiple components)
- `@pytest.mark.golden`: Golden tests for deterministic output verification
- `@pytest.mark.determinism`: Bit-identical determinism verification
- `@pytest.mark.property`: Property-based tests (Hypothesis)
- `@pytest.mark.schema`: Schema validation tests
- `@pytest.mark.qc`: Quality control tests
- `@pytest.mark.slow`: Slow tests (>5s execution time)
- `@pytest.mark.api`: External API tests (use mocks, not real API)
- `@pytest.mark.benchmark`: Performance benchmark tests

### Valid Examples — No network calls

```python
import pytest

@pytest.mark.unit
def test_normalize_chembl_id():
    """Unit test for ChEMBL ID normalization."""
    assert normalize_chembl_id("CHEMBL123") == "CHEMBL123"

@pytest.mark.golden
def test_activity_extraction_golden(golden_file):
    """Golden test for activity extraction output."""
    result = extract_activities(input_data)
    assert result == golden_file.read()

@pytest.mark.property
from hypothesis import given, strategies as st

@given(st.text(min_size=1, max_size=50))
def test_normalize_id_property(chembl_id: str):
    """Property-based test for ID normalization."""
    normalized = normalize_chembl_id(chembl_id)
    assert isinstance(normalized, str)
    assert len(normalized) > 0
```

## No Network Calls

Unit tests **MUST NOT** make actual network requests. Use mocks or fixtures
instead:

### Valid Examples — Mocked HTTP calls

```python
from unittest.mock import Mock, patch
import pytest


@pytest.mark.unit
@patch("requests.get")
def test_fetch_data_mocked(mock_get):
    """Test data fetching with mocked HTTP request."""
    mock_get.return_value.json.return_value = {"data": "test"}
    result = fetch_data("https://api.example.com")
    assert result == {"data": "test"}
    mock_get.assert_called_once()


@pytest.mark.unit
def test_process_data_with_fixture(sample_data_fixture):
    """Test data processing with fixture (no network)."""
    result = process_data(sample_data_fixture)
    assert len(result) > 0
```

### Invalid Examples — No network calls

```python
# Invalid: real network call in unit test
@pytest.mark.unit
def test_fetch_data_real():
    response = requests.get(
        "https://api.example.com/data"
    )  # SHALL NOT make real requests
    assert response.status_code == 200
```

## Golden Tests

Golden tests **MUST** be used for critical outputs to ensure deterministic
behavior:

### Golden Test Structure

1. Store expected output in `tests/golden/<area>/<version>/` (mirroring the
   `dataset/meta/qc/manifest` layout).
1. Compare actual output with the committed golden files (byte-wise for datasets/QC,
   structural comparison for metadata).
1. Update golden files only when intentional changes occur and document the reason.

### Valid Examples — Golden tests

```python
import pytest
from pathlib import Path


@pytest.mark.golden
def test_activity_schema_golden(golden_dir: Path):
    """Golden test for activity schema validation."""
    data = load_test_data()
    validated = validate_with_schema(data, ActivitySchema)

    output_file = golden_dir / "activity_validated.golden"
    if output_file.exists():
        expected = output_file.read_text()
        actual = validated.to_csv(index=False)
        assert actual == expected
    else:
        # First run: create golden file
        output_file.write_text(validated.to_csv(index=False))
```

## Property-Based Tests

Use Hypothesis for property-based testing of critical transformations:

### Valid Examples — Property-based tests

```python
from hypothesis import given, strategies as st
import pytest


@pytest.mark.property
@given(st.lists(st.floats(min_value=0.0, max_value=1000.0), min_size=1, max_size=100))
def test_normalize_values_property(values: list[float]):
    """Property-based test for value normalization."""
    normalized = normalize_values(values)
    assert len(normalized) == len(values)
    assert all(0.0 <= v <= 1.0 for v in normalized)
```

## Test Structure and Naming

### File Naming

Test files **MUST** follow the pattern: `test_*.py`

### Function Naming

Test functions **MUST** follow the pattern: `test_*`

### Class Naming

Test classes **MUST** follow the pattern: `Test*`

### Valid Examples — Naming

```python
# File: tests/bioetl/unit/test_normalizers.py

import pytest


class TestChemBLNormalizer:
    @pytest.mark.unit
    def test_normalize_id_valid(self):
        """Test normalization of valid ChEMBL ID."""
        assert normalize_chembl_id("CHEMBL123") == "CHEMBL123"

    @pytest.mark.unit
    def test_normalize_id_invalid(self):
        """Test normalization of invalid ChEMBL ID."""
        with pytest.raises(ValueError):
            normalize_chembl_id("INVALID")
```

## Coverage Requirements

### Minimum Coverage

Minimum coverage **MUST** be 85% for `src/bioetl` (enforced in CI).

### Coverage Exclusions

The following are excluded from coverage:

- `*/tests/*`
- `*/__pycache__/*`
- `def __repr__`
- `raise AssertionError`
- `raise NotImplementedError`
- `if __name__ == "__main__":`
- `if TYPE_CHECKING:`
- `@abstractmethod`

## Fixtures

### Shared Fixtures

Common fixtures **SHOULD** be defined in `tests/bioetl/conftest.py`:

```python
# tests/bioetl/conftest.py
import pytest
from pathlib import Path


@pytest.fixture
def sample_data_fixture():
    """Sample data fixture for testing."""
    return pd.DataFrame({"id": ["1", "2", "3"], "value": [1.0, 2.0, 3.0]})


@pytest.fixture
def golden_dir(tmp_path: Path) -> Path:
    """Directory for golden test files."""
    return tmp_path / "golden"
```

## Contract Tests

API clients **MUST** have contract tests to verify behavior:

### Valid Examples — Contract tests

```python
@pytest.mark.api
def test_api_client_contract(mock_api_server):
    """Contract test for API client."""
    client = APIClient(base_url=mock_api_server.url)
    result = client.fetch_data(endpoint="/data")
    assert result.status_code == 200
    assert "data" in result.json()
```

## Test Execution

### Running Tests

```bash
# Run all tests
pytest

# Run specific marker
pytest -m unit
pytest -m golden

# Run with coverage
pytest --cov=src/bioetl --cov-report=term-missing
```

### CI Configuration

Tests **MUST** pass in CI before PR merge:

- All unit tests
- Coverage ≥ 85%
- No network calls in unit tests
- Golden tests verify deterministic output

## References

- Pytest configuration: `pyproject.toml` (`[tool.pytest.ini_options]`)
- Coverage configuration: `pyproject.toml` (`[tool.coverage.*]`)
- Hypothesis documentation: [hypothesis.readthedocs.io](https://hypothesis.readthedocs.io/)
