# Semantic Scholar adapter

## Public API
- `from bioetl.sources.document.adapters.semantic_scholar import SemanticScholarAdapter`
- `from bioetl.sources.document.pipeline import DocumentPipeline` (enrichment stage integration)

## Configuration keys (`configs/pipelines/document.yaml`)
- `sources.semantic_scholar.enabled`
- `sources.semantic_scholar.base_url`
- `sources.semantic_scholar.api_key`
- `sources.semantic_scholar.batch_size`
- `sources.semantic_scholar.rate_limit_max_calls`
- `sources.semantic_scholar.rate_limit_period`
- `sources.semantic_scholar.workers`

## Environment variables
- `SEMANTIC_SCHOLAR_API_KEY` (optional)
