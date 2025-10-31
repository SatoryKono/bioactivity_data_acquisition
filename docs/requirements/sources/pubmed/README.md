# PubMed adapter

## Public API
- `from bioetl.sources.document.adapters.pubmed import PubMedAdapter`
- `from bioetl.sources.document.pipeline import DocumentPipeline` (enrichment stage integration)

## Configuration keys (`configs/pipelines/document.yaml`)
- `sources.pubmed.enabled`
- `sources.pubmed.base_url`
- `sources.pubmed.tool`
- `sources.pubmed.email`
- `sources.pubmed.api_key`
- `sources.pubmed.batch_size`
- `sources.pubmed.rate_limit_max_calls`
- `sources.pubmed.rate_limit_period`
- `sources.pubmed.workers`

## Environment variables
- `PUBMED_TOOL`
- `PUBMED_EMAIL`
- `PUBMED_API_KEY` (optional)
