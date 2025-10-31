# Crossref adapter

## Public API
- `from bioetl.sources.document.adapters.crossref import CrossrefAdapter`
- `from bioetl.sources.document.pipeline import DocumentPipeline` (enrichment stage integration)

## Configuration keys (`configs/pipelines/document.yaml`)
- `sources.crossref.enabled`
- `sources.crossref.base_url`
- `sources.crossref.mailto`
- `sources.crossref.batch_size`
- `sources.crossref.rate_limit_max_calls`
- `sources.crossref.rate_limit_period`
- `sources.crossref.workers`

## Environment variables
- `CROSSREF_MAILTO`
