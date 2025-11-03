# UniProt source

> **Note**: Implementation status: **planned**. All file paths referencing `src/bioetl/` in this document describe the intended architecture and are not yet implemented in the codebase.

## Public API

- `from bioetl.sources.uniprot import UniProtService`
- `from bioetl.sources.uniprot import UniProtEnrichmentResult`
- `from bioetl.pipelines.uniprot import UniProtPipeline`

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
