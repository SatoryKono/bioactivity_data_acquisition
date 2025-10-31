# UniProt source

## Public API
- `from bioetl.sources.uniprot import UniProtService`
- `from bioetl.sources.uniprot import UniProtEnrichmentResult`
- `from bioetl.pipelines.uniprot import UniProtPipeline`

### UniProt HTTP клиенты (package)
- `from bioetl.sources.uniprot.client.search_client import UniProtSearchClient`
- `from bioetl.sources.uniprot.client.idmapping_client import UniProtIdMappingClient`
- `from bioetl.sources.uniprot.client.orthologs_client import UniProtOrthologsClient`

Примечание: монолит `sources/uniprot/client.py` помечен к удалению. Для совместимости в пакетных клиентах добавлены шины: `search_client.UniProtSearchClient.fetch_entries(...)` и `idmapping_client.UniProtIdMappingClient.map_accessions(...)`. Для ортологов используйте адаптер `UniProtOrthologClientAdapter` из `client/orthologs_client.py`, если требуется датафрейм с приоритезацией.

## Module layout
- `src/bioetl/sources/uniprot/service.py` — enrichment orchestration (client calls, parsing, normalisation helpers)
- `src/bioetl/pipelines/uniprot.py` — standalone CLI pipeline wrapper
- `src/bioetl/sources/uniprot/pipeline.py` — compatibility proxy for the source registry

## Tests
- `tests/sources/uniprot/test_client.py` — HTTP client adapters (`fetch_entries`, ID mapping, ortholog lookups)
- `tests/sources/uniprot/test_parser.py` — parsing helpers and isoform expansion
- `tests/sources/uniprot/test_normalizer.py` — dataframe normalisation and enrichment fallbacks
- `tests/sources/uniprot/test_pipeline_e2e.py` — pipeline orchestration happy path

## Configuration keys (`configs/pipelines/uniprot.yaml`)
- `sources.uniprot.enabled`
- `sources.uniprot.base_url`
- `sources.uniprot.batch_size`
- `sources.uniprot_idmapping.enabled`
- `sources.uniprot_idmapping.base_url`
- `sources.uniprot_orthologs.enabled`
- `sources.uniprot_orthologs.base_url`

## Environment variables
- `UNIPROT_API_KEY` (optional; forwarded to `UnifiedAPIClient` when provided)
