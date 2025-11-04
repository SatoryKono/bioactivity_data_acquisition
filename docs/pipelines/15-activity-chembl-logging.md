# 15 Activity ChEMBL Logging

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes the structured logging format and mandatory fields for the Activity (ChEMBL) pipeline.

## Logging Format

All logs use structured JSON format via `structlog` with the following mandatory fields:

- `run_id`: Unique identifier for the pipeline run
- `stage`: Current pipeline stage (`extract`, `transform`, `validate`, `write`)
- `actor`: Pipeline name (`activity_chembl`)
- `source`: Data source identifier (`chembl`)
- `timestamp`: ISO-8601 UTC timestamp

## Stage-Specific Events

### Extract Stage

- `extraction_started`: Batch extraction begins
- `extraction_completed`: Batch extraction completes with row count
- `extraction_failed`: Extraction error with details

### Transform Stage

- `transformation_started`: Transformation begins
- `transformation_completed`: Transformation completes with statistics

### Validate Stage

- `validation_started`: Schema validation begins
- `validation_completed`: Validation passes with metrics
- `validation_failed`: Validation errors with details

### Write Stage

- `export_started`: File writing begins
- `export_completed`: All artifacts written successfully

## Related Documentation

- [Logging Overview](../logging/00-overview.md) — Logging system
- [Logging Guidelines](../styleguide/02-logging-guidelines.md) — Style guide
- [00-activity-chembl-overview.md](00-activity-chembl-overview.md) — Pipeline overview
