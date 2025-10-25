# Legacy Components

This directory contains deprecated components that have been replaced by the new v2 architecture.

## Moved Components

- `circuit_breaker.py` - Replaced by simplified retry logic in ChemblClient v2
- `fallback.py` - Replaced by HTTP-level fallback in ChemblClient v2  
- `graceful_degradation.py` - Simplified to explicit pipeline handling
- `cache_manager.py` - Replaced by direct TTLCache usage in clients

## Migration Notes

These components are kept for reference during the migration period but should not be used in new code. They will be removed in a future version.

For new implementations, use:
- `src/library/clients/chembl_v2.py` - New ChEMBL client
- `src/library/common/rate_limiter.py` - Token bucket rate limiting
- `src/library/config/models.py` - Pydantic configuration models
