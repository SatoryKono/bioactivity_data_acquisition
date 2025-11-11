# ChEMBL Source Architecture

This brief consolidates the planned component stack for every ChEMBL-facing pipeline so that pagination, parsing, normalisation and schema contracts stay aligned across clients.【F:docs/sources/01-interface-matrix.md†L7-L13】 Core behaviours inherit the generic source rules for paginators, parsers and HTTP clients, including deterministic stop conditions on empty responses and pure parsing logic.【F:docs/sources/00-sources-architecture.md†L31-L44】

## 1. Component matrix

| Entity | Client module/class | Pagination strategy | Parser | Normaliser | Schema output |
| --- | --- | --- | --- | --- | --- |
| Activity | `src/bioetl/clients/chembl_activity.py::ActivityChEMBLClient` | ID batching (`activity_id__in`) | `src/bioetl/sources/chembl/activity/parser/activity_parser.py::ActivityParser` | `src/bioetl/sources/chembl/activity/normalizer/activity_normalizer.py::ActivityNormalizer` | `src/bioetl/schemas/chembl_activity.py::ActivitySchema` |
| Assay | `src/bioetl/clients/chembl_assay.py::AssayChEMBLClient` | ID batching (`assay_chembl_id__in`) | `src/bioetl/sources/chembl/assay/parser/assay_parser.py::AssayParser` | `src/bioetl/sources/chembl/assay/normalizer/assay_normalizer.py::AssayNormalizer` | `src/bioetl/schemas/chembl_assay.py::AssaySchema` |
| Document | `src/bioetl/sources/chembl/document/client/document_client.py` | Recursive ID batching with POST overrides | `src/bioetl/sources/chembl/document/parser/document_parser.py::DocumentParser` | `src/bioetl/sources/chembl/document/normalizer/document_normalizer.py::DocumentNormalizer` | `src/bioetl/schemas/chembl_document.py::DocumentSchema` |
| Target | `src/bioetl/sources/chembl/target/client/target_client.py` | ID batching (`target_chembl_id__in`) | `src/bioetl/sources/chembl/target/parser/target_parser.py::TargetParser` | `src/bioetl/sources/chembl/target/normalizer/target_normalizer.py::TargetNormalizer` | `src/bioetl/schemas/chembl_target.py::TargetSchema` |
| TestItem | `src/bioetl/sources/chembl/testitem/client/testitem_client.py` | ID batching (`molecule_chembl_id__in`) | `src/bioetl/sources/chembl/testitem/parser/testitem_parser.py::TestItemParser` | `src/bioetl/sources/chembl/testitem/normalizer/testitem_normalizer.py::TestItemNormalizer` | `src/bioetl/schemas/chembl_testitem.py::TestItemSchema` |

_Source: `docs/sources/01-interface-matrix.md` (implementation status planned).【F:docs/sources/01-interface-matrix.md†L7-L13】_

## 2. Pagination and end-of-feed handling

### Activity

- Batches of 25 IDs are enforced both by configuration validation and the extractor loop, preventing URLs from exceeding ChEMBL's length limits.【F:docs/pipelines/06-activity-data-extraction.md†L162-L260】
- HTTP 204 responses mark the end of pagination and increment `page_empty`, ensuring the client stops once the API no longer returns results.【F:docs/pipelines/06-activity-data-extraction.md†L799-L812】

### Assay

- The pipeline mirrors the Activity batching contract: 25-ID chunks guarded by config validation, cached per release and retried in case of circuit-breaker trips.【F:docs/pipelines/05-assay-extraction.md†L11-L140】
- Empty or duplicate batches are disallowed by the shared paginator invariants (no duplicates, stop on empty page).【F:docs/sources/00-sources-architecture.md†L31-L35】

### Document

- Input IDs are chunked at 10 by default, automatically halved when long URLs or timeouts occur; POST overrides with `X-HTTP-Method-Override: GET` keep large requests compliant.【F:docs/pipelines/document-chembl/09-document-chembl-extraction.md†L170-L259】
- Recursive splitting retries half-batches after read timeouts, guaranteeing forward progress until single-record calls either succeed or raise, which is treated as EOF.【F:docs/pipelines/document-chembl/09-document-chembl-extraction.md†L223-L259】

### Target

- Targets use the same client/ID batching stack enumerated in the component matrix while exposing `target_chembl_id` as the pagination and business key, so empty pages trigger the shared paginator stop condition.【F:docs/sources/01-interface-matrix.md†L7-L13】【F:docs/pipelines/08-target-data-extraction.md†L31-L48】

### TestItem

- Molecule requests paginate via configurable 25-ID batches and flatten responses from `/molecule.json`, ensuring URL limits are not exceeded.【F:docs/pipelines/07-testitem-extraction.md†L32-L45】
- Optional PubChem enrichment is downstream of ChEMBL pagination; failures there do not reopen ChEMBL pagination, keeping EOF detection isolated to the core client.【F:docs/pipelines/07-testitem-extraction.md†L47-L69】

## 3. Parsing, normalisation and schema outputs

### Activity (parsing)

- Field mappings convert core API attributes such as `standard_value`, `standard_units`, BAO identifiers and ligand efficiency payloads into typed Pandas columns while deriving compound- and citation-level flags.【F:docs/pipelines/06-activity-data-extraction.md†L400-L459】
- The Pandera schema fixes column order, acceptable enumerations and numeric ranges, locking the normalised DataFrame to `ActivitySchema` contract version 1.0.0.【F:docs/pipelines/06-activity-data-extraction.md†L575-L609】

### Assay (parsing)

- Extraction expands nested assay parameters, variant sequences and classification arrays into deterministic columns before validation, with QC enforcing identifier patterns and duplicate checks.【F:docs/pipelines/05-assay-extraction.md†L11-L140】【F:docs/pipelines/05-assay-extraction.md†L700-L784】
- Sort order and column layout track `AssaySchema.Config.column_order`, ensuring the serializer can reindex reliably.【F:docs/pipelines/05-assay-extraction.md†L75-L85】

### Document (parsing)

- ChEMBL attributes (IDs, bibliographic metadata) map directly into normalised columns, while DOI cleaning, author counts and ISSN fields capture enrichment rules within the unified mapping table.【F:docs/pipelines/document-chembl/09-document-chembl-extraction.md†L378-L417】
- The extended `COLUMN_ORDER` enumerates identifiers, provenance flags and QC columns so the final DataFrame reindexes and hashes consistently prior to write.【F:docs/pipelines/document-chembl/09-document-chembl-extraction.md†L2044-L2085】

### Target (parsing)

- The target pipeline reuses the standard stack, surfacing enrichment hooks for UniProt/IUPHAR yet keeping `target_chembl_id` as the schema key to maintain compatibility with `TargetSchema`.【F:docs/pipelines/08-target-data-extraction.md†L31-L48】

### TestItem (parsing)

- Parsers flatten `molecule_hierarchy`, `molecule_properties`, structural fingerprints and synonym lists into ~80 flat columns, with the normaliser wiring optional PubChem-derived attributes into the same schema.【F:docs/pipelines/07-testitem-extraction.md†L32-L69】
- Component bindings guarantee that the schema and normaliser come from the ChemBL test item modules listed in the matrix.【F:docs/pipelines/07-testitem-extraction.md†L71-L78】【F:docs/sources/01-interface-matrix.md†L7-L13】

## 4. I/O contracts and validation hooks

- Document ingestion accepts unique `document_chembl_id` strings validated by a Pandera `DocumentInputSchema`, rejecting malformed IDs upfront and logging rejections for audit.【F:docs/pipelines/document-chembl/09-document-chembl-extraction.md†L170-L199】
- Assay ingestion mirrors that contract with `AssayInputSchema`, requiring `assay_chembl_id` and optional target filters before extraction begins.【F:docs/pipelines/05-assay-extraction.md†L37-L65】
- Activity outputs comply with the explicit Pandera schema and column order, providing deterministic ordering for downstream materialisation and hashing.【F:docs/pipelines/06-activity-data-extraction.md†L575-L609】
- Repository-wide HTTP defaults (timeouts, retries, rate limiting and headers) and materialisation paths are anchored by the base profile, so every client inherits consistent network/backoff and storage expectations.【F:configs/defaults/base.yaml†L4-L31】
- Deterministic hashing rules, float precision and column-order enforcement for final CSV/Parquet artefacts come from the determinism profile applied on top of pipeline configs.【F:configs/defaults/determinism.yaml†L2-L15】
- The interface matrix ties each parser to a parser unit test entry, documenting the expectation that parsing contracts are backed by automated tests before pipelines are promoted.【F:docs/sources/01-interface-matrix.md†L7-L13】
