# Dead-code sweep and naming alignment

- **Date:** 2025-11-14
- **Status:** Accepted
- **Deciders:** @bioetl-core
- **Tags:** refactoring, cleanup, naming

## Context

Prompt-driven refactors (1–7) left several unused helpers, fixtures, and ad-hoc naming stubs across `src/` and `tests/`. Vulture reports still contained test-only leftovers, and pseudo-stage placeholders violated PIPE-004 naming discipline. We needed a deterministic sweep that also preserved the repo invariants (Pandera schemas, deterministic outputs, strict typing).

## Decision

1. Extend `pyproject.toml` to keep `tool.ruff` scoped to `src/bioetl` and `tests`, and promote `vulture` to the dev-tooling stack so sweeps are reproducible.
2. Run `vulture` plus suppress-hint inventory (`ignore-hints.txt`), inspect the largest offenders, and remove unreferenced helpers (custom monkeypatch protocols, unused fixtures, dead fixture arguments) or annotate justified dynamic usages.
3. Enforce naming policy by deleting bespoke monkeypatch placeholders—tests rely on the canonical `pytest.MonkeyPatch`.
4. Track every removal in `artifacts/dead-code-pruned.csv` and store raw scanner outputs in `artifacts/` for auditing.

## Consequences

- Cleaner fixtures and helper signatures reduce suppression noise, yielding an empty `vulture` report at confidence 80.
- Tests now rely only on first-class pytest helpers; no pseudo modules remain, simplifying future migrations.
- Any future sweep must keep the CSV log and ADR in sync so that contract changes stay visible in reviews.
- Added maintenance cost: developers must rerun `vulture` and regenerate artifacts whenever new dead code appears.

## References

- `artifacts/vulture-report.txt`
- `artifacts/ignore-hints.txt`
- `artifacts/dead-code-pruned.csv`

