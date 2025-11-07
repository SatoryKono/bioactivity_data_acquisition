"""Vocabulary store integration helpers for schema modules."""
from __future__ import annotations

import os
from collections.abc import Iterable, Mapping
from functools import lru_cache
from pathlib import Path
from typing import Any

from bioetl.etl.vocab_store import (
    VocabStoreError,
    clear_vocab_store_cache,
    load_vocab_store,
)
from bioetl.etl.vocab_store import (
    get_ids as _get_ids,
)

VOCAB_STORE_ENV_VAR = "VOCAB_STORE"


def _default_vocab_path() -> Path:
    return Path(__file__).resolve().parents[3] / "configs" / "dictionaries"


def _resolve_vocab_path() -> Path:
    override = os.getenv(VOCAB_STORE_ENV_VAR)
    if override:
        return Path(override).expanduser().resolve()
    return _default_vocab_path()


@lru_cache(maxsize=1)
def _load_store() -> Mapping[str, Any]:
    path = _resolve_vocab_path()
    return load_vocab_store(path)


def refresh_vocab_store_cache() -> None:
    """Invalidate cached vocabulary store instances."""

    clear_vocab_store_cache()
    _load_store.cache_clear()


def vocab_store() -> Mapping[str, Any]:
    """Return the cached vocabulary store mapping."""

    return _load_store()


def vocab_ids(name: str, *, allowed_statuses: Iterable[str] | None = None) -> set[str]:
    """Return the set of allowed identifiers for the given dictionary name."""

    store = vocab_store()
    result = _get_ids(store, name, allowed_statuses=allowed_statuses)
    return set(result)


def required_vocab_ids(name: str, *, allowed_statuses: Iterable[str] | None = None) -> set[str]:
    """Return identifiers for ``name`` or raise a descriptive error."""

    try:
        values = vocab_ids(name, allowed_statuses=allowed_statuses)
    except VocabStoreError as exc:
        raise RuntimeError(
            f"Unable to load vocabulary '{name}'. "
            f"Set {VOCAB_STORE_ENV_VAR} or ensure dictionaries are available."
        ) from exc
    if not values:
        raise RuntimeError(
            f"Vocabulary '{name}' is empty. Update dictionary definitions before continuing."
        )
    return values


__all__ = [
    "VOCAB_STORE_ENV_VAR",
    "VocabStoreError",
    "refresh_vocab_store_cache",
    "required_vocab_ids",
    "vocab_ids",
    "vocab_store",
]

