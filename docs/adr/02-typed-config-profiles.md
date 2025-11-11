# ADR 002: Typed Configuration Profiles via Pydantic

- **Date:** 2024-05-05
- **Status:** Accepted
- **Deciders:** @data-platform, @ml-platform
- **Tags:** configuration, pydantic, environments

## Context

Pipelines require environment-specific settings (API credentials, rate limits, storage locations). Historically we relied on ad-hoc YAML files and environment variables. The documentation in `docs/configs/00-typed-configs-and-profiles.md` proposes Pydantic-based schemas with profile layering, but the implementation across pipelines is inconsistent. We need an authoritative decision to prevent drift and clarify how secrets, overrides, and validation are handled.

## Decision

- Adopt Pydantic models as the single source of truth for runtime configuration (`bioetl.config`).
- Each pipeline exposes a `Config` dataclass that inherits from shared base models and uses Pydantic validators for coercion.
- Profiles (e.g., `dev`, `staging`, `prod`) are stored in `configs/<pipeline>/<profile>.yaml` and loaded via a deterministic merge order: base → environment → overrides from CLI flags.
- Secrets never live in the profiles; instead we rely on environment variables resolved by `bioetl.config.load_env_secrets`.
- Validation errors fail fast at startup and surface in CI via `python -m bioetl.scripts.validate_configs`.

Alternatives (pure environment variables, Hydra) were rejected because they either lack schema validation or introduce a new dependency stack misaligned with existing tooling.

## Consequences

- Documentation and examples must point to the canonical loading function and highlight the merge order.
- New pipelines must add typed configs before wiring CLI entrypoints.
- We need regression tests to ensure configs remain serializable/deterministic across runs.
- Teams should add `Config` snapshots to `tests/bioetl/configs/` to catch unintended changes.

## References

- `docs/configs/00-typed-configs-and-profiles.md`
- `src/bioetl/config/__init__.py`
- `scripts/validate_configs.py`
