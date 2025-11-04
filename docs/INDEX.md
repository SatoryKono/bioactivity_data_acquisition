# Documentation Index

This document serves as the central navigation hub for all `bioetl` documentation.

## Core Concepts

- **[ETL Contract](etl_contract/00-etl-overview.md)**: An overview of the fundamental principles and architecture of the `bioetl` framework, including the `PipelineBase` contract.
- **[Source Architecture](sources/00-sources-architecture.md)**: Describes the layered component stack for data sources (Client, Parser, Normalizer) and the flow of data.
- **[Determinism Policy](determinism/00-determinism-policy.md)**: The specification for ensuring byte-for-byte reproducible outputs.

## Technical Specifications

- **[Typed Configurations](configs/00-typed-configs-and-profiles.md)**: A detailed look at the Pydantic-based configuration system, including profiles and layer merging.
- **[CLI Reference](cli/00-cli-overview.md)**: A guide to the Command-Line Interface, including architecture, commands, and flags.
- **[HTTP Clients](http/00-http-clients-and-retries.md)**: The specification for the unified HTTP client, including retries, backoff, and rate limiting.
- **[Structured Logging](logging/00-overview.md)**: An overview of the `structlog`-based logging system and its features.
- **[Quality Assurance](qc/00-qc-overview.md)**: The framework for data quality control, including metrics and Golden Tests.

## Pipeline Documentation

- **[ChEMBL Pipelines Catalog](pipelines/10-chembl-pipelines-catalog.md)**: A detailed catalog of all ChEMBL data extraction pipelines.
- **[Source Interface Matrix](sources/INTERFACE_MATRIX.md)**: A matrix mapping each pipeline to its specific component implementations.

## Style Guides

- **[Naming Conventions](styleguide/00-naming-conventions.md)**: The standard for naming documentation files.
- **[Python Code Style](styleguide/01-python-code-style.md)**: Code formatting, type annotations, and quality standards (ruff, black, mypy).
- **[Logging Guidelines](styleguide/02-logging-guidelines.md)**: Centralized logging with UnifiedLogger, structured JSON logs, and context enrichment.
- **[Data Schemas and Validation](styleguide/03-data-schemas.md)**: Pandera schema requirements, validation workflows, and schema versioning.
- **[Deterministic I/O](styleguide/04-deterministic-io.md)**: Atomic file writes, fixed ordering, UTC timestamps, and canonical serialization.
- **[Testing Standards](styleguide/05-testing-standards.md)**: Test markers, golden tests, property-based testing, and coverage requirements.
- **[CLI Contracts](styleguide/06-cli-contracts.md)**: Typer-based CLI, explicit flags, exit codes, and input validation.
- **[API Clients](styleguide/07-api-clients.md)**: UnifiedAPIClient usage, retry/backoff, throttling, circuit breakers, and caching.
- **[ETL Pipeline Architecture](styleguide/08-etl-architecture.md)**: One source one pipeline, unified components, star schema, and adapter pattern.
- **[Secrets and Configuration](styleguide/09-secrets-config.md)**: Secure secret management, typed Pydantic configs, and configuration profiles.
- **[Documentation Standards](styleguide/10-documentation-standards.md)**: Documentation synchronization, CHANGELOG, examples, and cross-references.

## Additional Resources

- **[Prompt Documentation](00-promt/)**: Original prompt specifications used for generating framework documentation. These files serve as reference materials for understanding the design decisions and requirements that shaped the `bioetl` framework.

---

## Complete File Listing

### ETL Contract (`etl_contract/`)

- [00-etl-overview.md](etl_contract/00-etl-overview.md) - Overview of the fundamental principles and architecture
- [01-pipeline-contract.md](etl_contract/01-pipeline-contract.md) - Pipeline contract specification
- [02-pipeline-config.md](etl_contract/02-pipeline-config.md) - Pipeline configuration details
- [03-extraction.md](etl_contract/03-extraction.md) - Data extraction contract
- [04-transformation-qc.md](etl_contract/04-transformation-qc.md) - Transformation and QC requirements
- [05-validation.md](etl_contract/05-validation.md) - Data validation specifications
- [06-determinism-output.md](etl_contract/06-determinism-output.md) - Deterministic output requirements
- [07-cli-integration.md](etl_contract/07-cli-integration.md) - CLI integration guidelines
- [08-implementation-guide.md](etl_contract/08-implementation-guide.md) - Implementation guide

### CLI (`cli/`)

- [00-cli-overview.md](cli/00-cli-overview.md) - CLI overview and principles
- [01-cli-commands.md](cli/01-cli-commands.md) - CLI commands reference
- [02-cli-exit_codes.md](cli/02-cli-exit_codes.md) - Exit codes specification

### Configurations (`configs/`)

- [00-typed-configs-and-profiles.md](configs/00-typed-configs-and-profiles.md) - Typed configurations and profiles

### Determinism (`determinism/`)

- [00-determinism-policy.md](determinism/00-determinism-policy.md) - Determinism policy specification

### HTTP Clients (`http/`)

- [00-http-clients-and-retries.md](http/00-http-clients-and-retries.md) - HTTP clients and retries specification

### Logging (`logging/`)

- [00-overview.md](logging/00-overview.md) - Logging system overview
- [01-public-api-and-configuration.md](logging/01-public-api-and-configuration.md) - Public API and configuration
- [02-structured-events-and-context.md](logging/02-structured-events-and-context.md) - Structured events and context
- [03-output-formats-and-determinism.md](logging/03-output-formats-and-determinism.md) - Output formats and determinism
- [04-security-secret-redaction.md](logging/04-security-secret-redaction.md) - Security and secret redaction
- [05-opentelemetry-integration.md](logging/05-opentelemetry-integration.md) - OpenTelemetry integration
- [06-usage-examples-and-best-practices.md](logging/06-usage-examples-and-best-practices.md) - Usage examples and best practices
- [07-testing-and-migration-guide.md](logging/07-testing-and-migration-guide.md) - Testing and migration guide

### Output (`output/`)

- [00-output-layout.md](output/00-output-layout.md) - Output layout specification

### Pipelines (`pipelines/`)

- [00-pipeline-base.md](pipelines/00-pipeline-base.md) - Pipeline base documentation
- [03-data-extraction.md](pipelines/03-data-extraction.md) - Data extraction pipeline
- [05-assay-chembl-extraction.md](pipelines/05-assay-chembl-extraction.md) - Assay ChEMBL extraction pipeline
- [06-activity-chembl-extraction.md](pipelines/06-activity-chembl-extraction.md) - Activity ChEMBL extraction pipeline
- [07-testitem-chembl-extraction.md](pipelines/07-testitem-chembl-extraction.md) - Testitem ChEMBL extraction pipeline
- [08-target-chembl-extraction.md](pipelines/08-target-chembl-extraction.md) - Target ChEMBL extraction pipeline
- [09-document-chembl-extraction.md](pipelines/09-document-chembl-extraction.md) - Document ChEMBL extraction pipeline
- [10-chembl-pipelines-catalog.md](pipelines/10-chembl-pipelines-catalog.md) - ChEMBL pipelines catalog
- [21-testitem-pubchem-extraction.md](pipelines/21-testitem-pubchem-extraction.md) - Testitem PubChem extraction pipeline
- [22-document-pubmed-extraction.md](pipelines/22-document-pubmed-extraction.md) - Document PubMed extraction pipeline
- [23-document-openalex-extraction.md](pipelines/23-document-openalex-extraction.md) - Document OpenAlex extraction pipeline
- [24-document-crossref-extraction.md](pipelines/24-document-crossref-extraction.md) - Document Crossref extraction pipeline
- [25-document-semantic-scholar-extraction.md](pipelines/25-document-semantic-scholar-extraction.md) - Document Semantic Scholar extraction pipeline

#### Pipeline Sources (`pipelines/sources/`)

- [crossref/README.md](pipelines/sources/crossref/README.md) - Crossref source documentation
- [openalex/README.md](pipelines/sources/openalex/README.md) - OpenAlex source documentation
- [pubmed/README.md](pipelines/sources/pubmed/README.md) - PubMed source documentation
- [semantic_scholar/README.md](pipelines/sources/semantic_scholar/README.md) - Semantic Scholar source documentation

> **Note**: ChEMBL, PubChem, UniProt, and IUPHAR source documentation has been consolidated into the main pipeline documents:
> - [ChEMBL Pipelines Catalog](pipelines/10-chembl-pipelines-catalog.md)
> - [PubChem TestItem Pipeline](pipelines/21-testitem-pubchem-extraction.md)
> - [UniProt Target Pipeline](pipelines/26-target-uniprot-extraction.md)
> - [IUPHAR Target Pipeline](pipelines/27-target-iuphar-extraction.md)
> - [ChEMBL to UniProt Mapping Pipeline](pipelines/28-chembl2uniprot-mapping.md)

### Quality Control (`qc/`)

- [00-qc-overview.md](qc/00-qc-overview.md) - QC overview
- [01-metrics-catalog.md](qc/01-metrics-catalog.md) - Metrics catalog
- [02-golden-tests.md](qc/02-golden-tests.md) - Golden tests specification
- [03-checklists-and-ci.md](qc/03-checklists-and-ci.md) - Checklists and CI integration

### Schemas (`schemas/`)

- [00-pandera-policy.md](schemas/00-pandera-policy.md) - Pandera policy specification

### Sources (`sources/`)

- [00-sources-architecture.md](sources/00-sources-architecture.md) - Sources architecture overview
- [INTERFACE_MATRIX.md](sources/INTERFACE_MATRIX.md) - Interface matrix mapping
- [chembl/00-architecture.md](sources/chembl/00-architecture.md) - ChEMBL source architecture

### Style Guide (`styleguide/`)

- [00-naming-conventions.md](styleguide/00-naming-conventions.md) - Naming conventions

### Prompt Documentation (`00-promt/`)

- [00-01-etl-contract.md](00-promt/00-01-etl-contract.md) - ETL contract prompt
- [00-02-logging-system.md](00-promt/00-02-logging-system.md) - Logging system prompt
- [00-03-pipeline-base-core-orchestrator.md](00-promt/00-03-pipeline-base-core-orchestrator.md) - Pipeline base core orchestrator prompt
- [00-04-deterministic-policy-of-downloads.md](00-promt/00-04-deterministic-policy-of-downloads.md) - Deterministic policy of downloads prompt
- [00-05-typer-cli.md](00-promt/00-05-typer-cli.md) - Typer CLI prompt
- [00-06-type-safe-configs-and-inclusion-layers.md](00-promt/00-06-type-safe-configs-and-inclusion-layers.md) - Type-safe configs and inclusion layers prompt
- [00-07-pandera-schemas-and-rigorous-data-validation.md](00-promt/00-07-pandera-schemas-and-rigorous-data-validation.md) - Pandera schemas and rigorous data validation prompt
- [00-08-http-clients-with-backoff-and-unified-request-rules.md](00-promt/00-08-http-clients-with-backoff-and-unified-request-rules.md) - HTTP clients with backoff and unified request rules prompt
- [00-09-source-architecture.md](00-promt/00-09-source-architecture.md) - Source architecture prompt
- [00-10-a-complete-set-of-chembl-pipelines.md](00-promt/00-10-a-complete-set-of-chembl-pipelines.md) - Complete set of ChEMBL pipelines prompt

## How to Maintain Consistency

1.  When changing code, update the corresponding documentation sections and ensure all `[ref]` links are accurate.
2.  Before committing, run the project's QA scripts (e.g., `npx markdownlint-cli2 "**/*.md"`).
3.  If adding a new source, expand the tables in the `[ref: repo:docs/sources/INTERFACE_MATRIX.md@refactoring_001]` and the `[ref: repo:docs/pipelines/10-chembl-pipelines-catalog.md@refactoring_001]`.
