# API Reference

This document provides comprehensive API reference for the Bioactivity Data Acquisition library.

## Core Modules

### Activity Module

The activity module provides functionality for extracting and processing bioactivity data from ChEMBL.

#### ActivityConfig

Configuration class for activity data processing.

```python
from library.activity import ActivityConfig

config = ActivityConfig.from_yaml("config.yaml")
```

#### ActivityPipeline

Main pipeline class for activity data processing.

```python
from library.activity import ActivityPipeline

pipeline = ActivityPipeline(config)
result = pipeline.run()
```

### Target Module

The target module handles target data extraction and enrichment.

#### TargetConfig

Configuration for target data processing.

```python
from library.target import TargetConfig

config = TargetConfig.from_yaml("config.yaml")
```

#### TargetPipeline

Pipeline for target data processing.

```python
from library.target import TargetPipeline

pipeline = TargetPipeline(config)
result = pipeline.run()
```

### Assay Module

The assay module provides assay data processing capabilities.

#### AssayConfig

Configuration for assay data processing.

```python
from library.assay import AssayConfig

config = AssayConfig.from_yaml("config.yaml")
```

#### AssayPipeline

Pipeline for assay data processing.

```python
from library.assay import AssayPipeline

pipeline = AssayPipeline(config)
result = pipeline.run()
```

### Testitem Module

The testitem module handles molecular data processing.

#### TestitemConfig

Configuration for testitem data processing.

```python
from library.testitem import TestitemConfig

config = TestitemConfig.from_yaml("config.yaml")
```

#### TestitemPipeline

Pipeline for testitem data processing.

```python
from library.testitem import TestitemPipeline

pipeline = TestitemPipeline(config)
result = pipeline.run()
```

## Configuration Classes

### Runtime Settings

Common runtime settings for all pipelines.

```python
from library.config import RuntimeSettings

runtime = RuntimeSettings(
    workers=4,
    timeout_sec=60,
    retries=3
)
```

### Cache Settings

Configuration for caching mechanisms.

```python
from library.config import CacheSettings

cache = CacheSettings(
    enabled=True,
    ttl=3600,
    max_size=1000
)
```

### HTTP Settings

HTTP client configuration.

```python
from library.config import HTTPSettings

http = HTTPSettings(
    timeout_sec=30,
    retries=3,
    backoff_factor=1.0
)
```

## Data Models

### Activity Data

Activity data model with validation.

```python
from library.schemas.activity_schema import ActivitySchema

activity = ActivitySchema(
    molecule_chembl_id="CHEMBL123",
    target_chembl_id="CHEMBL456",
    activity_value=10.5,
    activity_unit="nM"
)
```

### Target Data

Target data model.

```python
from library.schemas.target_schema import TargetSchema

target = TargetSchema(
    target_chembl_id="CHEMBL456",
    target_name="Example Target",
    target_type="SINGLE PROTEIN"
)
```

### Assay Data

Assay data model.

```python
from library.schemas.assay_schema import AssaySchema

assay = AssaySchema(
    assay_chembl_id="CHEMBL789",
    assay_type="B",
    assay_description="Example assay"
)
```

## Utility Functions

### Data Validation

Validate data against schemas.

```python
from library.utils.validation import validate_data

is_valid = validate_data(data, schema)
```

### Data Transformation

Transform data between formats.

```python
from library.utils.transform import transform_data

transformed = transform_data(data, source_format="csv", target_format="json")
```

### Caching

Cache management utilities.

```python
from library.utils.cache import CacheManager

cache = CacheManager(ttl=3600)
cached_data = cache.get_or_set("key", fetch_function)
```

## Error Handling

### Custom Exceptions

The library defines custom exceptions for different error types.

```python
from library.exceptions import (
    APIError,
    ValidationError,
    ConfigurationError
)

try:
    # API call
    pass
except APIError as e:
    print(f"API error: {e}")
```

### Retry Logic

Built-in retry mechanisms for API calls.

```python
from library.utils.retry import retry_with_backoff

@retry_with_backoff(max_retries=3, backoff_factor=2.0)
def api_call():
    # API call implementation
    pass
```

## Logging

### Structured Logging

The library uses structured logging for better observability.

```python
from library.logging_setup import configure_logging

logger = configure_logging(
    level="INFO",
    file_enabled=True,
    console_format="json"
)

logger.info("Processing started", batch_size=100)
```

### Log Context

Add context to log messages.

```python
from library.logging_setup import bind_stage

with bind_stage(logger, "data_processing"):
    logger.info("Processing batch", batch_id=123)
```

## Performance Optimization

### Batch Processing

Process data in batches for better performance.

```python
from library.utils.batch import process_batches

results = process_batches(
    data,
    batch_size=100,
    processor_function=process_batch
)
```

### Parallel Processing

Use parallel processing for CPU-intensive tasks.

```python
from library.utils.parallel import parallel_map

results = parallel_map(
    data,
    processor_function,
    max_workers=4
)
```

## Configuration Examples

### Basic Configuration

```python
config = TestitemConfig(
    runtime=TestitemRuntimeSettings(
        cache_enabled=True,
        cache_ttl=3600
    )
)
```
