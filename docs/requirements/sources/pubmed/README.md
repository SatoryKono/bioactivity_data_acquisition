# PubMed adapter

## Public API

- `from bioetl.sources.document.adapters.pubmed import PubMedAdapter`
- `from bioetl.sources.chembl.document.pipeline import DocumentPipeline` (enrichment stage integration; совместимость старого API — `bioetl.pipelines.document.DocumentPipeline`)

## CLI entrypoint

- Обогащение PubMed доступно через общий пайплайн `document`.

  Запускайте: `python -m bioetl.cli.main document --mode all` (или `chembl` для
  отключения внешних адаптеров) с конфигурацией `configs/pipelines/document.yaml`.
  Отдельной команды CLI для PubMed не предусмотрено.

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
