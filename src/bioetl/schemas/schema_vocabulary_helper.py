"""Совместимый фасад для работы со словарями из схем."""

from __future__ import annotations

import os
from collections.abc import Iterable, Mapping
from functools import lru_cache
from pathlib import Path
from typing import Any

from bioetl.core.utils.vocab_store import (
    VocabStoreError,
    clear_vocab_store_cache,
)
from bioetl.core.utils.vocab_store import (
    get_ids as _core_get_ids,
)
from bioetl.core.utils.vocab_store import (
    load_vocab_store as _core_load_vocab_store,
)

VOCAB_STORE_ENV_VAR = "VOCAB_STORE"
load_vocab_store = _core_load_vocab_store
_get_ids = _core_get_ids


def _default_vocab_path() -> Path:
    """Return the default dictionaries directory relative to the repo root."""

    return Path(__file__).resolve().parents[3] / "configs" / "dictionaries"


def _resolve_vocab_path() -> Path:
    """Resolve the vocabulary store path honoring the override environment variable."""

    override = os.getenv(VOCAB_STORE_ENV_VAR)
    if override:
        return Path(override).expanduser().resolve()
    return _default_vocab_path()


@lru_cache(maxsize=1)
def _load_cached_store() -> Mapping[str, Any]:
    """Load and memoize the vocabulary store mapping."""

    path = _resolve_vocab_path()
    return load_vocab_store(path)


def refresh_vocab_store_cache() -> None:
    """Invalidate cached vocabulary store contents."""

    clear_vocab_store_cache()
    _load_cached_store.cache_clear()


def required_vocab_ids(
    name: str,
    *,
    allowed_statuses: Iterable[str] | None = None,
) -> set[str]:
    """Return identifiers for ``name`` ensuring the dictionary is non-empty."""

    try:
        values = _get_ids(_load_cached_store(), name, allowed_statuses=allowed_statuses)
    except VocabStoreError as exc:
        message = (
            f"Unable to load vocabulary '{name}'. "
            f"Set {VOCAB_STORE_ENV_VAR} or ensure dictionaries are available."
        )
        raise RuntimeError(message) from exc

    normalized = set(str(value) for value in values)
    if not normalized:
        message = (
            f"Vocabulary '{name}' is empty. Update dictionary definitions before continuing."
        )
        raise RuntimeError(message)
    return normalized


__all__ = [
    "VOCAB_STORE_ENV_VAR",
    "VocabStoreError",
    "load_vocab_store",
    "_get_ids",
    "refresh_vocab_store_cache",
    "required_vocab_ids",
]

