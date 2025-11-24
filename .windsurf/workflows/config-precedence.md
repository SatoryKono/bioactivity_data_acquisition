---
trigger: model_decision
description: USE WHEN merging configuration; precedence is env > CLI > config files > profiles
---

# Config Precedence

> Scope:
> - USE WHEN merging configuration; precedence is env > CLI > config files > profiles
> - Use when editing files matching: `src/**/config*.py`, `src/**/*.py`

## Mandatory

- Apply configuration in this order: environment overrides > CLI > config files > profile defaults.

## Reference

See [docs/styleguide/09-secrets-config.md](../../docs/styleguide/09-secrets-config.md)
