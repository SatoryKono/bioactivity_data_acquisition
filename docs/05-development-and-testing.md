# Development and testing

This document explains how to develop and test BioETL code while
respecting the core project invariants.

It complements the project-wide rules and focuses on practical
recommendations for local development and CI.

## Core invariants

Development and tests must respect the same invariants as production
runs:

- Deterministic I/O
  - Stable row and column ordering in all outputs.
  - Canonical serialization (for example JSON with sorted keys).
  - Atomic writes (tmp file -> fsync -> rename).
  - Timestamps stored in ISO-8601 UTC format.
- Validate-before-write
  - Every table is validated against a Pandera schema before being
    written.
  - Schemas use `strict=True`, `ordered=True`, and `coerce=True`.
- Structured logging
  - Logging goes through UnifiedLogger, not `print` or raw logging.
  - A shared run context is initialized once and passed to all log
    records.

## Testing strategy

BioETL uses several categories of tests. Common pytest markers include:

- `unit`
  - Fast tests for pure business logic without network or heavy I/O.
- `integration`
  - Tests that cover pipeline wiring, I/O, and interactions between
    components.
- `golden`
  - Tests that compare current outputs and QC artifacts to stored golden
    artifacts byte-for-byte.
- `determinism`
  - Tests that ensure repeated runs with the same inputs and config
    produce identical outputs.
- `schema` and `qc`
  - Tests focused on schema validation and QC artifact formats.
- `api` and `benchmark`
  - HTTP-related tests (usually mocked) and performance measurements
    where applicable.

Unit tests must not perform real network calls; external dependencies
are mocked at the client layer.

## Coverage and CI gates

CI enforces minimum coverage and blocks merges when tests or checks
fail. Locally, developers are expected to:

- Run focused test subsets during development.
- Use coverage reports to ensure that new code paths are exercised.

A conceptual example for running tests with coverage:

```bash
pytest tests/ --cov=src/bioetl --cov-report=term-missing
```

Exact commands may vary, but the goal is to keep coverage for
`src/bioetl` at or above the threshold enforced in CI.

## Writing new tests

When adding new functionality:

- Prefer unit tests for domain logic
  - Test transformations and business rules in isolation from I/O.
- Add integration tests for critical pipeline paths
  - Cover end-to-end behavior for key pipelines or stages.
- Add golden tests for critical datasets
  - Capture representative outputs and QC artifacts as golden files.
  - Use the `--golden` flag in CLI or helpers so that CI can compare
    against these artifacts.
- Use property-based tests where transformations have clear invariants
  - For example, idempotent normalization steps or monotonic mappings.

## Working on pipelines

When modifying a pipeline:

1. Check whether the change affects public data contracts
   (schemas, column sets, or QC formats).
2. If contracts change, update:
   - Pandera schemas in the central registry.
   - Documentation under `docs/schemas/` and `docs/qc/` as needed.
   - Golden artifacts used by critical tests.
3. Add or update tests to cover the new behavior.
4. Run at least the relevant unit, integration, and golden tests before
   opening a pull request.

## Local development loop

A typical local development loop looks like this:

1. Make a small, focused code change.
2. Run targeted unit tests for the affected modules.
3. If pipeline behavior or outputs changed, run the relevant golden
   tests or a golden CLI run.
4. Update documentation in `docs/` when contracts, CLI behavior, or QC
   formats change.
5. Before pushing, run the broader test suite and any required
   linters or formatters.

Following this loop helps keep behavior deterministic, contracts
well-documented, and CI green.
