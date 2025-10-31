# OpenAlex adapter

## Public API
- `from bioetl.sources.document.adapters.openalex import OpenAlexAdapter`
- `from bioetl.sources.document.pipeline import DocumentPipeline` (enrichment stage integration)

## Configuration keys (`configs/pipelines/document.yaml`)
- `sources.openalex.enabled`
- `sources.openalex.base_url`
- `sources.openalex.batch_size`
- `sources.openalex.rate_limit_max_calls`
- `sources.openalex.rate_limit_period`
- `sources.openalex.workers`

## Environment variables
- none (static profile)
