# Architecture Overview

BioETL follows a layered architecture with clear boundaries between
orchestration, domain, and infrastructure code.

## Layers

- Orchestration layer: bioetl.pipelines
  - Defines concrete pipelines and their configuration.
  - Orchestrates the standard pipeline lifecycle.
- Domain layer: bioetl.schemas and related domain logic
  - Defines typed Pandera schemas and domain-specific transformations.
  - Encodes business rules and validation logic.
- Infrastructure layer: bioetl.clients and I/O utilities
  - Implements HTTP clients, storage backends, and low-level utilities.
  - Provides reusable building blocks for pipelines.

The dependency direction is strictly from orchestration down to domain
and infrastructure, never the other way around.

## Pipeline lifecycle

Every pipeline follows the same high-level lifecycle:

1. bootstrap
2. extract
3. transform
4. validate
5. write
6. postprocess
7. teardown

Custom business logic is implemented only in the relevant stages, while
cross-cutting concerns (logging, schema validation, I/O) are handled by
shared components.

## Core invariants

- Deterministic I/O: identical inputs and configuration must produce
  byte-identical outputs.
- Validate-before-write: every table is validated against a Pandera
  schema with strict=True and ordered=True before it is written.
- Structured logging: all logs are emitted via UnifiedLogger with a
  shared run context (pipeline code, run_id, stage, dataset, source).
- QC and golden artifacts: each dataset is accompanied by meta.yaml,
  quality reports, and optional golden files that are used in CI to
  guard against regressions.
