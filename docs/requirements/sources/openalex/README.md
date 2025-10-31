# OpenAlex adapter

## Public API
- `from bioetl.sources.document.adapters.openalex import OpenAlexAdapter`
- `from bioetl.sources.chembl.document.pipeline import DocumentPipeline` (enrichment stage integration; совместимость старого API — `bioetl.pipelines.document.DocumentPipeline`)

## CLI entrypoint
- Адаптер OpenAlex исполняется через `python -m bioetl.cli.main document`.
  Управляйте его активацией настройкой `sources.openalex.enabled` в
  `configs/pipelines/document.yaml`. Дополнительной команды CLI не требуется.

## Configuration keys (`configs/pipelines/document.yaml`)
- `sources.openalex.enabled`
- `sources.openalex.base_url`
- `sources.openalex.batch_size`
- `sources.openalex.rate_limit_max_calls`
- `sources.openalex.rate_limit_period`
- `sources.openalex.workers`

## Environment variables
- none (static profile)
