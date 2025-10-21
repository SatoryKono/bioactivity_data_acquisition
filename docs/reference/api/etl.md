# ETL API Reference

This document provides detailed API reference for the ETL (Extract, Transform, Load) functionality in the Bioactivity Data Acquisition library.

## Overview

The ETL module provides a comprehensive framework for extracting, transforming, and loading bioactivity data from various sources including ChEMBL, PubChem, and other biological databases.

## Core ETL Classes

### ETLPipeline

The main ETL pipeline class that orchestrates the entire data processing workflow.

```python
from library.etl.pipeline import ETLPipeline

pipeline = ETLPipeline(config)
result = pipeline.run()
```

#### Methods

- `run()`: Execute the complete ETL pipeline
- `extract()`: Extract data from sources
- `transform()`: Transform extracted data
- `load()`: Load transformed data to destination

### DataExtractor

Base class for data extraction from various sources.

```python
from library.etl.extract import DataExtractor

extractor = DataExtractor(config)
data = extractor.extract()
```

### DataTransformer

Base class for data transformation operations.

```python
from library.etl.transform import DataTransformer

transformer = DataTransformer(config)
transformed_data = transformer.transform(data)
```

### DataLoader

Base class for loading data to various destinations.

```python
from library.etl.load import DataLoader

loader = DataLoader(config)
loader.load(data)
```

## Source-Specific Extractors

### ChEMBLExtractor

Extract data from ChEMBL database.

```python
from library.etl.extract import ChEMBLExtractor

extractor = ChEMBLExtractor(
    base_url="https://www.ebi.ac.uk/chembl/api/data",
    timeout=30
)
data = extractor.extract_molecules(molecule_ids)
```

### PubChemExtractor

Extract data from PubChem database.

```python
from library.etl.extract import PubChemExtractor

extractor = PubChemExtractor(
    base_url="https://pubchem.ncbi.nlm.nih.gov/rest/pug",
    timeout=30
)
data = extractor.extract_compounds(compound_ids)
```

### IUPHARExtractor

Extract data from IUPHAR database.

```python
from library.etl.extract import IUPHARExtractor

extractor = IUPHARExtractor(
    base_url="https://www.guidetopharmacology.org/services",
    timeout=30
)
data = extractor.extract_targets(target_ids)
```

## Data Transformation

### Normalization

Normalize data to standard formats.

```python
from library.etl.transform import DataNormalizer

normalizer = DataNormalizer()
normalized_data = normalizer.normalize(data)
```

### Validation

Validate data against schemas.

```python
from library.etl.transform import DataValidator

validator = DataValidator(schema)
is_valid = validator.validate(data)
```

### Enrichment

Enrich data with additional information.

```python
from library.etl.transform import DataEnricher

enricher = DataEnricher()
enriched_data = enricher.enrich(data)
```

## Data Loading

### CSV Loader

Load data to CSV files.

```python
from library.etl.load import CSVLoader

loader = CSVLoader(output_dir="data/output")
loader.load(data, filename="results.csv")
```

### JSON Loader

Load data to JSON files.

```python
from library.etl.load import JSONLoader

loader = JSONLoader(output_dir="data/output")
loader.load(data, filename="results.json")
```

### Database Loader

Load data to database.

```python
from library.etl.load import DatabaseLoader

loader = DatabaseLoader(connection_string="sqlite:///data.db")
loader.load(data, table_name="results")
```

## Quality Control

### QC Metrics

Calculate quality control metrics.

```python
from library.etl.qc import QCMetrics

metrics = QCMetrics()
qc_results = metrics.calculate(data)
```

### Correlation Analysis

Perform correlation analysis on data.

```python
from library.etl.qc import CorrelationAnalyzer

analyzer = CorrelationAnalyzer()
correlations = analyzer.analyze(data)
```

### Data Profiling

Profile data for quality assessment.

```python
from library.etl.qc import DataProfiler

profiler = DataProfiler()
profile = profiler.profile(data)
```

## Error Handling

### Retry Mechanisms

Built-in retry logic for failed operations.

```python
from library.etl.utils import retry_with_backoff

@retry_with_backoff(max_retries=3, backoff_factor=2.0)
def extract_data():
    # Extraction logic
    pass
```

### Circuit Breaker

Circuit breaker pattern for external API calls.

```python
from library.etl.utils import CircuitBreaker

breaker = CircuitBreaker(failure_threshold=5, timeout=60)
data = breaker.call(api_function)
```

## Performance Monitoring

### Metrics Collection

Collect performance metrics during ETL operations.

```python
from library.etl.metrics import PerformanceMetrics

metrics = PerformanceMetrics()
with metrics.timer("extraction"):
    data = extract_data()
    
print(f"Время извлечения: {metrics.get_duration('extraction')}")
```
