# 00 Testing Strategy

**Version:** 1.0.0 **Date:** 2025-01-29 **Author:** Data Acquisition Team

## Purpose

This document describes the testing strategy for ChEMBL pipelines, focusing on centralized testing of common patterns (`extract_by_ids`, `fetch_chembl_release`) and comprehensive coverage of edge cases.

## Testing Structure

### Test Organization

```
tests/
├── bioetl/
│   └── pipelines/
│       └── chembl/
│           ├── common/
│           │   ├── test_extract_by_ids_base.py    # Centralized extract_by_ids tests
│           │   ├── test_fetch_chembl_release.py   # Centralized release fetching tests
│           │   ├── test_pipeline_base.py          # Base pipeline tests
│           │   └── test_batch_helper.py           # Batch extraction tests
│           ├── activity/
│           │   └── test_pipeline.py               # Activity-specific tests
│           ├── assay/
│           │   └── test_pipeline.py               # Assay-specific tests
│           ├── document/
│           │   └── test_pipeline.py               # Document-specific tests
│           ├── target/
│           │   └── test_pipeline.py               # Target-specific tests
│           └── testitem/
│               └── test_pipeline.py               # TestItem-specific tests
└── support/
    ├── factories.py                               # Test data factories
    └── chembl_fixtures.py                         # ChEMBL-specific fixtures
```

## Test Categories

### 1. Centralized Tests (`test_extract_by_ids_base.py`)

**Purpose:** Parameterized tests for the common `extract_by_ids` pattern used across all ChEMBL pipelines.

**Coverage:**
- Happy path: valid IDs, single and multiple batches
- Edge cases: empty lists, single ID, duplicates, whitespace handling
- Error handling: invalid IDs, network errors, timeouts
- Configuration: dry-run mode, limit enforcement, batch processing

**Example:**
```python
@pytest.mark.parametrize(
    ("pipeline_cls", "id_column", "sample_ids"),
    [
        (target_run.ChemblTargetPipeline, "target_chembl_id", ["CHEMBL1", "CHEMBL2"]),
        (assay_run.ChemblAssayPipeline, "assay_chembl_id", ["CHEMBL100", "CHEMBL101"]),
        # ... other pipelines
    ],
)
def test_extract_by_ids_happy_path(...):
    """Parameterized test for successful extraction."""
```

### 2. Release Fetching Tests (`test_fetch_chembl_release.py`)

**Purpose:** Comprehensive tests for ChEMBL release version fetching logic.

**Coverage:**
- Happy path: ChemblClient handshake, UnifiedAPIClient direct HTTP
- Edge cases: missing fields, empty responses, alternative field names
- Error handling: network errors, timeouts, HTTP errors, invalid JSON
- Special cases: TestItem pipeline special handling (chembl_db_version)

**Example:**
```python
def test_fetch_chembl_release_via_chembl_client(...):
    """Test release fetching through ChemblClient.handshake()."""
    mock_client.handshake.return_value = {"chembl_db_version": "33"}
    result = pipeline.fetch_chembl_release(mock_client)
    assert result == "33"
```

### 3. Pipeline-Specific Tests

**Purpose:** Tests for pipeline-specific logic and transformations.

**Coverage:**
- Normalization: identifier normalization, string field normalization
- Transformations: domain-specific data transformations
- Enrichment: enrichment logic specific to each pipeline
- Edge cases: empty dataframes, missing columns, invalid data

## Test Fixtures

### ChEMBL Fixtures (`tests/support/chembl_fixtures.py`)

**Available Fixtures:**
- `mock_chembl_bundle`: Mock bundle for ChEMBL entity clients
- `mock_chembl_bundle_with_data`: Bundle with pre-configured data
- `sample_ids`: Example valid ChEMBL IDs
- `id_column`: Default ID column name

**Usage:**
```python
def test_example(mock_chembl_bundle, pipeline_config_fixture, run_id):
    pipeline = ChemblTargetPipeline(config=pipeline_config_fixture, run_id=run_id)
    with patch.object(pipeline, "build_chembl_entity_bundle", return_value=mock_chembl_bundle):
        result = pipeline.extract_by_ids(["CHEMBL1"])
```

## Coverage Requirements

### Coverage Thresholds

- **Overall project:** ≥65% (configured in `pyproject.toml`)
- **ChEMBL pipelines:** ≥85% (target for critical paths)
- **Base classes:** ≥90% (shared logic must be well-tested)

### Coverage Measurement

```bash
# Full coverage report
pytest --cov=src/bioetl/pipelines/chembl --cov=src/bioetl/chembl/common \
       --cov-report=html --cov-report=term-missing \
       tests/bioetl/pipelines/chembl/

# Focus on extract_by_ids and fetch_chembl_release
pytest --cov=src/bioetl/pipelines/chembl --cov=src/bioetl/chembl/common \
       --cov-report=term-missing \
       -k "extract_by_ids or fetch_chembl_release" \
       tests/bioetl/pipelines/chembl/

# Coverage with threshold check
pytest --cov=src/bioetl/pipelines/chembl \
       --cov-report=term-missing \
       --cov-fail-under=85 \
       tests/bioetl/pipelines/chembl/
```

### Key Coverage Areas

1. **`src/bioetl/chembl/common/descriptor.py`:**
   - `ChemblPipelineBase.fetch_chembl_release`
   - `ChemblPipelineBase.run_batched_extraction`
   - `ChemblPipelineBase.extract_by_ids` (if common implementation exists)

2. **`src/bioetl/pipelines/chembl/*/run.py`:**
   - Methods `extract_by_ids` for each pipeline
   - Methods `fetch_chembl_release` (if overridden)

## Test Scenarios

### extract_by_ids Scenarios

| Scenario | Input | Expected Result | Test Location |
|----------|-------|----------------|---------------|
| Valid IDs, single batch | `["CHEMBL1", "CHEMBL2"]` | DataFrame with 2 rows | `test_extract_by_ids_base.py` |
| Valid IDs, multiple batches | `["CHEMBL1", ..., "CHEMBL100"]` | DataFrame with all records | `test_extract_by_ids_base.py` |
| Empty list | `[]` | Empty DataFrame | `test_extract_by_ids_base.py` |
| Duplicates | `["CHEMBL1", "CHEMBL1", "CHEMBL2"]` | Unique IDs only | `test_extract_by_ids_base.py` |
| Invalid IDs | `["INVALID", "NOT_CHEMBL"]` | Empty or NA values | Pipeline-specific tests |
| Dry-run mode | Any IDs | Empty DataFrame | `test_extract_by_ids_base.py` |
| Limit enforcement | Many IDs with limit=2 | Max 2 rows | `test_extract_by_ids_base.py` |

### fetch_chembl_release Scenarios

| Scenario | Input | Expected Result | Test Location |
|----------|-------|----------------|---------------|
| ChemblClient handshake | `{"chembl_db_version": "33"}` | `"33"` | `test_fetch_chembl_release.py` |
| UnifiedAPIClient HTTP | `{"chembl_db_version": "34"}` | `"34"` | `test_fetch_chembl_release.py` |
| Network error | ConnectionError | `None`, warning logged | `test_fetch_chembl_release.py` |
| Missing field | `{"api_version": "1.0"}` | `None` | `test_fetch_chembl_release.py` |
| Empty response | `{}` | `None` | `test_fetch_chembl_release.py` |
| Alternative field | `{"chembl_release": "35"}` | `"35"` | `test_fetch_chembl_release.py` |

## Best Practices

### 1. Use Parameterization

Parameterize tests that check the same behavior across multiple pipelines:

```python
@pytest.mark.parametrize("pipeline_cls", [
    target_run.ChemblTargetPipeline,
    assay_run.ChemblAssayPipeline,
])
def test_common_behavior(pipeline_cls, ...):
    """Test shared behavior across pipelines."""
```

### 2. Mock External Dependencies

Always mock external API calls and file I/O:

```python
with patch.object(pipeline, "build_chembl_entity_bundle", return_value=mock_bundle):
    result = pipeline.extract_by_ids(ids)
```

### 3. Test Edge Cases

Cover empty inputs, invalid data, network errors, and boundary conditions.

### 4. Use Fixtures

Leverage shared fixtures from `tests/support/chembl_fixtures.py` for consistency.

### 5. Maintain Test Independence

Each test should be independent and not rely on state from other tests.

## Running Tests

### Run All ChEMBL Pipeline Tests

```bash
pytest tests/bioetl/pipelines/chembl/
```

### Run Centralized Tests Only

```bash
pytest tests/bioetl/pipelines/chembl/common/test_extract_by_ids_base.py
pytest tests/bioetl/pipelines/chembl/common/test_fetch_chembl_release.py
```

### Run Pipeline-Specific Tests

```bash
pytest tests/bioetl/pipelines/chembl/target/
pytest tests/bioetl/pipelines/chembl/activity/
```

### Run with Coverage

```bash
pytest --cov=src/bioetl/pipelines/chembl --cov-report=html tests/bioetl/pipelines/chembl/
```

## Related Documentation

- [Pipeline Base Tests](../pipelines/chembl/common/test_pipeline_base.py) — Base pipeline functionality
- [Batch Helper Tests](../pipelines/chembl/common/test_batch_helper.py) — Batch extraction logic
- [Pipeline Documentation](../pipelines/chembl/) — Pipeline-specific documentation

