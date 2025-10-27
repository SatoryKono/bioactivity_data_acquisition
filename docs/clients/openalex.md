# OpenAlex Client Documentation

## Overview

The OpenAlex client provides access to the OpenAlex API for retrieving scholarly works metadata. It includes comprehensive fallback mechanisms to ensure reliable data retrieval even when primary lookup methods fail.

## Fallback Strategy Hierarchy

The OpenAlex client implements a multi-tier fallback strategy:

1. **Primary Lookup**: Direct API call using DOI or PMID
2. **Filter API Fallback**: Search using OpenAlex filter API
3. **Search API Fallback**: General search API as secondary fallback
4. **Title-based Search**: Search by article title when all other methods fail

### DOI Lookup Fallback Chain

```
DOI Direct Lookup → Filter API (doi:{doi}) → Title Search (if title provided)
```

### PMID Lookup Fallback Chain

```
PMID Direct Lookup → Filter API (pmid:{pmid}) → Search API (search:{pmid}) → Title Search (if title provided)
```

## Configuration

### Fallback Strategy Configuration

The OpenAlex client uses `OpenAlexFallbackStrategy` with the following default settings:

```python
fallback_config = FallbackConfig(
    max_retries=1,           # Conservative retry policy
    base_delay=5.0,          # 5 second base delay
    max_delay=30.0,          # Maximum 30 second delay
    backoff_multiplier=1.5,  # Moderate backoff multiplier
    jitter=True              # Add randomness to delays
)
```

### Retry Logic

- **Rate Limiting (429)**: Retry once with 5-30 second delays
- **Server Errors (500-504)**: Retry once with 8-30 second delays  
- **Other Errors (404, etc.)**: No retries, immediate fallback

## API Methods

### `fetch_by_doi(doi: str, title: str | None = None)`

Fetches a work by DOI with comprehensive fallback support.

**Parameters:**
- `doi`: DOI identifier (e.g., "10.1000/test")
- `title`: Optional article title for fallback search

**Fallback Sequence:**
1. Direct DOI lookup: `https://api.openalex.org/works/https://doi.org/{doi}`
2. Filter API: `https://api.openalex.org/works?filter=doi:{doi}`
3. Title search: `https://api.openalex.org/works?search={title}` (if title provided)

### `fetch_by_pmid(pmid: str, title: str | None = None)`

Fetches a work by PMID with comprehensive fallback support.

**Parameters:**
- `pmid`: PubMed ID (e.g., "12345678")
- `title`: Optional article title for fallback search

**Fallback Sequence:**
1. Direct PMID lookup: `https://api.openalex.org/works/pmid:{pmid}`
2. Filter API: `https://api.openalex.org/works?filter=pmid:{pmid}`
3. Search API: `https://api.openalex.org/works?search={pmid}`
4. Title search: `https://api.openalex.org/works?search={title}` (if title provided)

## Error Handling

### Rate Limiting (429)

When rate limited, the client:
- Logs a warning with rate limit information
- Returns empty record with error message
- Suggests getting an API key for higher limits

```python
{
    "source": "openalex",
    "openalex_title": None,
    "openalex_error": "Rate limited: 429 Too Many Requests. Consider getting an API key."
}
```

### Server Errors (500-504)

Server errors trigger:
- One retry attempt with exponential backoff
- Fallback to alternative lookup methods
- Empty record if all methods fail

### Not Found Errors (404)

When works are not found:
- Automatic fallback to next method in sequence
- Title-based search as final fallback
- Empty record with descriptive error message

## Graceful Degradation

The client includes `OpenAlexDegradationStrategy` for graceful degradation:

### Degradation Triggers

- Rate limiting (429)
- Server errors (500-504)
- API unavailability

### Degraded Response Structure

```python
{
    "source": "fallback",
    "api": "openalex",
    "fallback_reason": "openalex_unavailable",
    "degraded": True,
    "openalex_error": "Error description",
    # All OpenAlex fields set to None
    "openalex_title": None,
    "openalex_year": None,
    # ... other fields
}
```

## Usage Examples

### Basic Usage

```python
from library.clients.openalex import OpenAlexClient
from library.config import APIClientConfig

config = APIClientConfig(base_url="https://api.openalex.org")
client = OpenAlexClient(config)

# Fetch by DOI
result = client.fetch_by_doi("10.1000/test")
print(result["openalex_title"])

# Fetch by PMID
result = client.fetch_by_pmid("12345678")
print(result["openalex_year"])
```

### With Title Fallback

```python
# Provide title for enhanced fallback
result = client.fetch_by_doi("10.1000/test", title="Test Article Title")
result = client.fetch_by_pmid("12345678", title="Test Article Title")
```

### Batch Processing

```python
# Batch DOI processing
dois = ["10.1000/test1", "10.1000/test2", "10.1000/test3"]
results = client.fetch_by_dois_batch(dois)

# Batch PMID processing  
pmids = ["12345678", "87654321", "11223344"]
results = client.fetch_by_pmids_batch(pmids)
```

## Response Schema

The client returns standardized response objects with the following structure:

```python
{
    "source": "openalex",
    "openalex_doi": "10.1000/test",
    "openalex_title": "Article Title",
    "openalex_doc_type": "journal-article",
    "openalex_crossref_doc_type": "journal-article",
    "openalex_year": 2023,
    "openalex_pmid": "12345678",
    "openalex_abstract": "Article abstract...",
    "openalex_issn": "1234-5678",
    "openalex_authors": ["John Doe", "Jane Smith"],
    "openalex_journal": "Journal Name",
    "openalex_volume": "15",
    "openalex_issue": "3",
    "openalex_first_page": "123",
    "openalex_last_page": "145",
    "openalex_error": None  # Set if error occurred
}
```

## Logging

The client provides comprehensive logging for debugging and monitoring:

### Log Levels

- **INFO**: Successful operations, fallback transitions
- **WARNING**: Rate limiting, fallback usage, missing data
- **ERROR**: API failures, unexpected errors
- **DEBUG**: Detailed request/response information

### Key Log Messages

- `openalex_fetch_by_doi`: DOI lookup attempt
- `openalex_doi_fallback`: DOI fallback triggered
- `openalex_title_fallback`: Title search fallback
- `openalex_rate_limited`: Rate limiting detected
- `openalex_title_search_success`: Successful title search

## Best Practices

1. **Always provide titles** when available for enhanced fallback
2. **Monitor rate limiting** and consider API key for production use
3. **Handle empty records** gracefully in your application
4. **Use batch methods** for multiple lookups to improve efficiency
5. **Check error fields** to understand why lookups failed

## Troubleshooting

### Common Issues

**Rate Limiting**: Get an API key or implement longer delays between requests

**Empty Results**: Check if DOI/PMID exists in OpenAlex database, try title fallback

**Server Errors**: Implement retry logic with exponential backoff

**Missing Fields**: Some works may not have complete metadata in OpenAlex

### Debug Mode

Enable debug logging to see detailed request/response information:

```python
import logging
logging.getLogger("library.clients.openalex").setLevel(logging.DEBUG)
```
