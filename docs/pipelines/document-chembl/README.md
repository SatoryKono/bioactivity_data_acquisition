# Document ChEMBL Pipeline

The document_chembl pipeline models documents that support activity and
assay records, such as scientific publications.

## Purpose

- Represent documents (for example, journal articles) that provide
  evidence for bioactivity data.
- Link documents to activities, assays, and targets.
- Provide a basis for document-level analyses and derived entities (such
  as terms or similarities).

## Inputs and sources

- Primary source: ChEMBL document tables.
- Optional enrichment from external literature metadata providers such
  as OpenAlex or Semantic Scholar, depending on pipeline configuration.

## Outputs and schemas

The pipeline produces document-centric datasets validated using Pandera
schemas from the bioetl.schemas registry.

Field-level details for document-related datasets are described in
../../datatypes/document.md and related datatype files (for example,
../../datatypes/document_term.md or
../../datatypes/document_similarity.md).

## Notes

Any HTTP-based enrichment logic must use unified API clients with
timeouts, retries, throttling, and caching, as required by project
rules.
