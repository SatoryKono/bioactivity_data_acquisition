"""Stub migration registry loader for schema package.

The production repository may ship compiled migrations; this placeholder keeps
imports functional inside the refactoring branch until dedicated migrations are
implemented.
"""

from __future__ import annotations


def load_builtin_migrations() -> None:
    """Register built-in schema migrations (no-op on this branch)."""

    return None

