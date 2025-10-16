# Integration Tests

This directory contains integration tests for the bioactivity data acquisition pipeline. These tests make real API calls to external services and require network access.

## Running Integration Tests

### Prerequisites

1. **Network Access**: Tests require internet connectivity to access external APIs
2. **API Keys** (optional): Some tests require API keys for full functionality:
   - `CHEMBL_API_TOKEN`: For ChEMBL API access
   - `PUBMED_API_KEY`: For PubMed API access
   - `SEMANTIC_SCHOLAR_API_KEY`: For Semantic Scholar API access

### Running Tests

Integration tests are skipped by default. To run them, use the `--run-integration` flag:

```bash
# Run all integration tests
pytest tests/integration/ --run-integration

# Run specific integration test
pytest tests/integration/test_chembl_integration.py --run-integration

# Run with verbose output
pytest tests/integration/ --run-integration -v

# Run slow tests (includes performance and sustained usage tests)
pytest tests/integration/ --run-integration -m "not slow"

# Run only slow tests
pytest tests/integration/ --run-integration -m slow
```

### Test Categories

#### 1. ChEMBL Integration Tests (`test_chembl_integration.py`)
- **API Status Check**: Verifies ChEMBL API is accessible
- **Compound Search**: Tests compound search functionality
- **Activity Search**: Tests activity search functionality
- **Rate Limiting**: Verifies rate limiting is enforced
- **Error Handling**: Tests error handling for invalid requests
- **Timeout Handling**: Tests timeout behavior
- **Large Dataset**: Tests handling of larger datasets (marked as slow)
- **Pagination**: Tests pagination functionality

#### 2. Pipeline Integration Tests (`test_pipeline_integration.py`)
- **Minimal Pipeline**: Tests pipeline with minimal configuration
- **Real Data**: Tests pipeline with real document data
- **Error Handling**: Tests pipeline error handling with invalid data
- **Configuration Validation**: Tests pipeline configuration validation
- **Multiple Sources**: Tests pipeline with different API sources
- **Performance**: Tests pipeline performance with larger datasets (marked as slow)

#### 3. API Limits Integration Tests (`test_api_limits_integration.py`)
- **Rate Limit Enforcement**: Tests rate limiting with strict limits
- **Concurrent Rate Limiting**: Tests rate limiting with concurrent requests
- **API Error Handling**: Tests handling of various API errors
- **Timeout Handling**: Tests timeout behavior with real network conditions
- **Retry Mechanism**: Tests retry mechanism configuration
- **Different API Endpoints**: Tests different API endpoints and their rate limits
- **Response Size Limits**: Tests handling of large API responses
- **Sustained Usage**: Tests sustained API usage over time (marked as slow)

## Test Configuration

Integration tests use a minimal configuration that:

- Limits API calls to prevent rate limiting issues
- Uses small page sizes and limited records
- Sets conservative timeouts
- Uses single-threaded processing for predictable behavior

## Environment Variables

Set these environment variables for full test coverage:

```bash
export CHEMBL_API_TOKEN="your_chembl_token"
export PUBMED_API_KEY="your_pubmed_key"
export SEMANTIC_SCHOLAR_API_KEY="your_semantic_scholar_key"
```

## Test Markers

- `@pytest.mark.integration`: Marks tests as integration tests
- `@pytest.mark.slow`: Marks tests that take longer to run
- `skip_if_no_api_key`: Skips test if required API keys are missing
- `skip_if_no_network`: Skips test if network access is not available

## Troubleshooting

### Common Issues

1. **Rate Limiting**: If tests fail due to rate limiting, wait a few minutes and try again
2. **Network Timeouts**: Ensure stable internet connection
3. **API Key Issues**: Verify API keys are valid and have appropriate permissions
4. **Missing Dependencies**: Ensure all required packages are installed

### Debug Mode

Run tests with debug output:

```bash
pytest tests/integration/ --run-integration -v -s --tb=short
```

### Test Data

Integration tests create temporary test data and clean up automatically. If tests fail unexpectedly, check for:

- Temporary files left in the system
- Network connectivity issues
- API service availability

## Continuous Integration

Integration tests are designed to be run in CI environments with:

- Network access enabled
- API keys configured as secrets
- Reasonable timeouts for CI systems
- Graceful handling of API unavailability

## Contributing

When adding new integration tests:

1. Use appropriate test markers (`@pytest.mark.integration`)
2. Add skip conditions for missing dependencies
3. Clean up temporary data
4. Handle API errors gracefully
5. Add docstrings explaining what the test validates
6. Consider performance impact (use `@pytest.mark.slow` for long-running tests)
