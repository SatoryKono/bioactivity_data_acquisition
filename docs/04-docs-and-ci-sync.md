# Docs and CI synchronization

This document explains how documentation under `docs/` is kept in sync
with the codebase and project rules using CI checks.

It complements the project-wide rules and focuses on practical
expectations for documentation updates.

## CI checks for documentation

CI workflows include jobs that verify the consistency and health of
project documentation.

Examples include:

- Link and format checks for markdown files.
- Synchronization checks between documentation and IDE command bundles
  (for example `.windsurf/commands`).
- Additional checks defined in workflows such as
  `.github/workflows/doc-sync-check.yml`.

Exact workflow definitions may evolve over time; always refer to the
YAML files under `.github/workflows/` for the current configuration.

## Command and rules registries

Documentation must remain consistent with:

- IDE command bundles under `.windsurf/commands/`
  - For example, commands such as `/run-activity-chembl`,
    `/run-chembl-all`, or `/validate-config`.
- Project rules under `.windsurf/rules/`
  - Core invariants, naming conventions, docs structure, and other
    cross-cutting policies.

When contracts change (for example, new CLI commands, flags, or changes
in pipeline behavior), both the code and the relevant documentation
should be updated in the same change set.

## When documentation must be updated

Developers should update documentation whenever they:

- Change public data contracts
  - Pandera schemas, column sets, or QC artifact formats.
- Add or modify CLI commands or flags
  - New run commands, new options, or changed semantics.
- Change output artifact formats
  - `meta.yaml` structure, QC report CSV layouts, JSON QC keys, or
    correlation reports.
- Adjust core development practices
  - Testing strategy, coverage targets, or determinism guarantees.

Relevant documentation lives primarily under:

- `docs/schemas/` for data schemas.
- `docs/qc/` for QC artifacts.
- `docs/cli/` for CLI and configuration behavior.
- `docs/05-development-and-testing.md` for development and testing
  practices.

## Typical CI workflow

A typical CI pipeline will:

1. Install dependencies and run code linters.
2. Run unit, integration, golden, and determinism tests.
3. Run documentation-related checks (for example link checks or
   doc-sync checks).

If documentation is missing, inconsistent with code, or violates naming
or structure rules, the CI pipeline should fail, blocking merges until
issues are resolved.

## Local checks before pushing

Before opening a pull request, developers are encouraged to:

1. Review `git diff` for both code and `docs/` changes together.
2. Ensure that any changes to CLI behavior, schemas, or QC artifacts are
   reflected in the corresponding docs.
3. Run a representative subset of tests and, where available, any local
   doc or link check commands.

Keeping documentation and CI checks aligned helps ensure that public
contracts remain clear, enforced, and discoverable over time.
