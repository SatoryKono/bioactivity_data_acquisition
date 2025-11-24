"""Shared helpers for resolving vocabulary store locations."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Final

from infrastructure.schemas.common import default_schema_path

VOCAB_STORE_ENV_VAR: Final = "VOCAB_STORE"


def default_vocab_store_path() -> Path:
    """Return the default dictionaries directory relative to the repository root."""

    return default_schema_path("dictionaries")


def resolve_vocab_store_path(path: str | Path | None = None) -> Path:
    """Resolve the vocabulary store path with environment overrides.

    If ``path`` is provided, it is expanded and resolved. Otherwise the function
    checks the :data:`VOCAB_STORE_ENV_VAR` environment variable and falls back to
    the default dictionaries directory.
    """

    if path is not None:
        return Path(path).expanduser().resolve()

    override = os.getenv(VOCAB_STORE_ENV_VAR)
    if override:
        return Path(override).expanduser().resolve()

    return default_vocab_store_path()


__all__ = [
    "VOCAB_STORE_ENV_VAR",
    "default_vocab_store_path",
    "resolve_vocab_store_path",
]
