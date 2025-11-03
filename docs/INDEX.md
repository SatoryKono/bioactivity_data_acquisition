# Documentation Index

This document serves as the central navigation hub for all `bioetl` documentation.

## Core Concepts

- **[ETL Contract](etl_contract/00-etl-overview.md)**: An overview of the fundamental principles and architecture of the `bioetl` framework, including the `PipelineBase` contract.
- **[Source Architecture](sources/00-sources-architecture.md)**: Describes the layered component stack for data sources (Client, Parser, Normalizer) and the flow of data.
- **[Determinism Policy](determinism/01-determinism-policy.md)**: The specification for ensuring byte-for-byte reproducible outputs.

## Technical Specifications

- **[Typed Configurations](configs/00-typed-configs-and-profiles.md)**: A detailed look at the Pydantic-based configuration system, including profiles and layer merging.
- **[CLI Reference](cli/00-cli-overview.md)**: A guide to the Command-Line Interface, including architecture, commands, and flags.
- **[HTTP Clients](http/00-http-clients-and-retries.md)**: The specification for the unified HTTP client, including retries, backoff, and rate limiting.
- **[Structured Logging](logging/00-overview.md)**: An overview of the `structlog`-based logging system and its features.
- **[Quality Assurance](qc/00-qc-overview.md)**: The framework for data quality control, including metrics and Golden Tests.

## Pipeline Documentation

- **[ChEMBL Pipelines Catalog](pipelines/10-chembl-pipelines-catalog.md)**: A detailed catalog of all ChEMBL data extraction pipelines.
- **[Source Interface Matrix](sources/INTERFACE_MATRIX.md)**: A matrix mapping each pipeline to its specific component implementations.

## How to Maintain Consistency

1.  When changing code, update the corresponding documentation sections and ensure all `[ref]` links are accurate.
2.  Before committing, run the project's QA scripts (e.g., `npx markdownlint-cli2 "**/*.md"`).
3.  If adding a new source, expand the tables in the `[ref: repo:sources/INTERFACE_MATRIX.md@refactoring_001]` and the `[ref: repo:pipelines/10-chembl-pipelines-catalog.md@refactoring_001]`.
