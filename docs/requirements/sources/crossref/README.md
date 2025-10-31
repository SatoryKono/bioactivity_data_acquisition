# Crossref adapter

## Public API
- `from bioetl.sources.document.adapters.crossref import CrossrefAdapter`
- `from bioetl.sources.chembl.document.pipeline import DocumentPipeline` (enrichment stage integration; совместимость старого API — `bioetl.pipelines.document.DocumentPipeline`)

## CLI entrypoint
- Внешнее обогащение Crossref запускается через публичный пайплайн `document`:
  `python -m bioetl.cli.main document --config configs/pipelines/document.yaml`.
  Отдельной команды CLI для Crossref нет; включение контролируется флагом
  `sources.crossref.enabled` в конфигурации документного пайплайна.

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
