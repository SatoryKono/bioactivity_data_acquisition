# Data Extraction Refactoring Migration Guide

## Overview

This document describes the migration from the old data extraction architecture to the new v2 system that provides better determinism, performance, and maintainability.

## Key Changes

### 1. New ChEMBL Client Architecture

**Before (v1):**

```python
from library.clients.chembl import ChEMBLClient
from library.config import APIClientConfig

config = APIClientConfig()
client = ChEMBLClient(config)
```

**After (v2):**

```python
from library.clients.chembl_v2 import ChemblClient
from library.config.models import ApiCfg

api_cfg = ApiCfg()
client = ChemblClient(api=api_cfg)
```

### 2. Configuration Models

**Before:**

```python
# Complex configuration with multiple layers
config = APIClientConfig(
    base_url="https://www.ebi.ac.uk/chembl/api/data",
    timeout=30.0,
    retries=3,
    # ... many other parameters
)
```

**After:**
```python
# Clean Pydantic models
from library.config.models import ApiCfg, RetryCfg, ChemblCacheCfg

api_cfg = ApiCfg(
    base_url="https://www.ebi.ac.uk/chembl/api/data",
    timeout_connect=10.0,
    timeout_read=30.0,
    retries=3,
    backoff_jitter_seed=42  # For determinism!
)
```

### 3. Rate Limiting

**Before:**
```python
# Complex rate limiting with multiple components
from library.clients.base import RateLimiter, RateLimitConfig

limiter = RateLimiter(RateLimitConfig(max_calls=5, period=1.0))
```

**After:**
```python
# Simple token bucket rate limiter
from library.common.rate_limiter import get_limiter

limiter = get_limiter("chembl", rps=5.0, burst=10)
```

### 4. Pipeline Usage

**Before:**
```python
from library.target.pipeline import TargetPipeline

# Complex retry logic and error handling
for payloads, raw_df, parsed_df in iter_target_batches(
    ids, cfg=cfg, client=client, mapping_cfg=mapping_cfg
):
    # Process data
```

**After:**
```python
from library.target.pipeline import TargetPipeline

# Simplified interface with built-in error handling
payloads, raw_df, parsed_df = fetch_targets(
    ids,
    cfg=api_cfg,
    client=client,
    mapping_cfg=mapping_cfg,
    chunk_size=5
)
```

## Migration Steps

### Step 1: Update Imports

Replace old imports with new ones:

```python
# OLD
from library.clients.chembl import ChEMBLClient
from library.clients.base import BaseApiClient
from library.target.pipeline import TargetPipeline

# NEW
from library.clients.chembl_v2 import ChemblClient
from library.clients.base_v2 import BaseApiClient
from library.target.pipeline import TargetPipeline
```

### Step 2: Update Configuration

Convert old configuration to new Pydantic models:

```python
# OLD
config = APIClientConfig(
    base_url="https://www.ebi.ac.uk/chembl/api/data",
    timeout=30.0,
    retries=3,
    backoff_multiplier=2.0,
    # ... other parameters
)

# NEW
from library.config.models import ApiCfg, RetryCfg, ChemblCacheCfg

api_cfg = ApiCfg(
    base_url="https://www.ebi.ac.uk/chembl/api/data",
    timeout_connect=10.0,
    timeout_read=30.0,
    retries=3,
    backoff_multiplier=2.0,
    backoff_jitter_seed=42  # Critical for determinism!
)

retry_cfg = RetryCfg(
    retries=3,
    backoff_multiplier=2.0,
    backoff_jitter_seed=42,
    max_delay=60.0
)

chembl_cfg = ChemblCacheCfg(
    cache_ttl=3600,
    cache_size=1000
)
```

### Step 3: Update Client Initialization

```python
# OLD
client = ChEMBLClient(config)

# NEW
client = ChemblClient(
    api=api_cfg,
    retry=retry_cfg,
    chembl=chembl_cfg
)
```

### Step 4: Update Pipeline Code

```python
# OLD
for payloads, raw_df, parsed_df in iter_target_batches(
    ids,
    cfg=cfg,
    client=client,
    mapping_cfg=mapping_cfg,
    chunk_size=5,
    timeout=30.0,
    enable_split_fallback=True
):
    # Process each batch
    process_batch(payloads, raw_df, parsed_df)

# NEW
payloads, raw_df, parsed_df = fetch_targets(
    ids,
    cfg=api_cfg,
    client=client,
    mapping_cfg=mapping_cfg,
    chunk_size=5,
    timeout=30.0,
    enable_split_fallback=True
)
# Process all data at once
process_all_data(payloads, raw_df, parsed_df)
```

## Backward Compatibility

For gradual migration, use the compatibility adapter:

```python
from library.clients.chembl_adapter import ChemblClientAdapter

# This provides the old interface while using the new implementation
adapter = ChemblClientAdapter(config=old_config)
result = adapter.get_chembl_status()
```

## Breaking Changes

### 1. Removed Components

The following components have been moved to `library/legacy/` and should not be used:

- `circuit_breaker.py` - Replaced by simplified retry logic
- `fallback.py` - Replaced by HTTP-level fallback
- `graceful_degradation.py` - Simplified to explicit pipeline handling
- `cache_manager.py` - Replaced by direct TTLCache usage

### 2. Configuration Changes

- `APIClientConfig` → `ApiCfg`, `RetryCfg`, `ChemblCacheCfg`
- `timeout` → `timeout_connect` and `timeout_read`
- Added `backoff_jitter_seed` for determinism

### 3. Client Interface Changes

- `_request()` → `fetch()` (for ChEMBL client)
- Removed circuit breaker and fallback manager dependencies
- Simplified error handling

## Benefits

### 1. Determinism

The new system provides deterministic results:

```python
# Multiple runs produce identical outputs
result1 = fetch_targets(ids, cfg=api_cfg, client=client, mapping_cfg=mapping_cfg)
result2 = fetch_targets(ids, cfg=api_cfg, client=client, mapping_cfg=mapping_cfg)

# CSV outputs are byte-identical
csv1 = result1[2].to_csv(index=False)
csv2 = result2[2].to_csv(index=False)
assert csv1 == csv2
```

### 2. Performance

- Token bucket rate limiting is more efficient
- TTL cache reduces redundant requests
- Simplified retry logic reduces overhead

### 3. Maintainability

- Clean separation of concerns
- Pydantic models provide validation
- Reduced complexity (removed 7+ helper classes)

## Testing

Run the determinism tests to verify your migration:

```bash
pytest tests/test_determinism.py -v
```

These tests verify that:
- Multiple runs produce identical results
- JSON serialization is deterministic
- CSV outputs have identical SHA256 hashes

## Troubleshooting

### Common Issues

1. **Import Errors**: Make sure to update all imports to use the new modules
2. **Configuration Errors**: Use Pydantic models instead of old config classes
3. **Determinism Issues**: Ensure `backoff_jitter_seed` is set to a fixed value

### Getting Help

If you encounter issues during migration:

1. Check the test suite for examples
2. Use the compatibility adapter for gradual migration
3. Review the legacy components in `library/legacy/` for reference

## Future Plans

- Remove legacy components in next major version
- Add more deterministic features
- Improve streaming support for large datasets
