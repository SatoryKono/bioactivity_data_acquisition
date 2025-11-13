"""Vocabulary store accessors decoupled from schema definitions."""

from __future__ import annotations

import os
from collections.abc import Iterable, Mapping
from functools import lru_cache
from pathlib import Path
from typing import Any

from bioetl.core.utils.vocab_store import (
    VocabStoreError,
    clear_vocab_store_cache as _clear_vocab_store_cache,
    get_ids as _get_ids,
    load_vocab_store as _load_vocab_store,
)

VOCAB_STORE_ENV_VAR = "VOCAB_STORE"


def _default_vocab_path() -> Path:
    return Path(__file__).resolve().parents[2] / "configs" / "dictionaries"


def _resolve_vocab_path() -> Path:
    override = os.getenv(VOCAB_STORE_ENV_VAR)
    if override:
        return Path(override).expanduser().resolve()
    return _default_vocab_path()


@lru_cache(maxsize=1)
def _load_store() -> Mapping[str, Any]:
    path = _resolve_vocab_path()
    return _load_vocab_store(path)


def vocab_store() -> Mapping[str, Any]:
    """Return the cached vocabulary store mapping."""

    return _load_store()


def refresh_vocab_cache() -> None:
    """Invalidate cached vocabulary store instances."""

    _clear_vocab_store_cache()
    _load_store.cache_clear()


def vocab_ids(name: str, *, allowed_statuses: Iterable[str] | None = None) -> set[str]:
    """Return identifiers for ``name`` without enforcing presence."""

    result = _get_ids(vocab_store(), name, allowed_statuses=allowed_statuses)
    return set(result)


def required_vocab_ids(
    name: str,
    *,
    allowed_statuses: Iterable[str] | None = None,
) -> set[str]:
    """Return identifiers for ``name`` or raise a descriptive error."""

    try:
        values = vocab_ids(name, allowed_statuses=allowed_statuses)
    except VocabStoreError as exc:
        message = (
            f"Unable to load vocabulary '{name}'. "
            f"Set {VOCAB_STORE_ENV_VAR} or ensure dictionaries are available."
        )
        raise RuntimeError(message) from exc

    if not values:
        message = (
            f"Vocabulary '{name}' is empty. Update dictionary definitions before continuing."
        )
        raise RuntimeError(message)
    return values


class VocabularyService:
    """Service wrapper providing vocabulary lookup operations."""

    def __init__(self) -> None:
        self._store_cache = vocab_store

    def get_store(self) -> Mapping[str, Any]:
        """Return the cached vocabulary store mapping."""

        return self._store_cache()

    def ids(self, name: str, *, allowed_statuses: Iterable[str] | None = None) -> set[str]:
        """Return identifiers for ``name`` without enforcing presence."""

        return vocab_ids(name, allowed_statuses=allowed_statuses)

    def required_ids(
        self,
        name: str,
        *,
        allowed_statuses: Iterable[str] | None = None,
    ) -> set[str]:
        """Return identifiers for ``name`` and ensure the collection is non-empty."""

        return required_vocab_ids(name, allowed_statuses=allowed_statuses)


@lru_cache(maxsize=1)
def get_vocabulary_service() -> VocabularyService:
    """Return the singleton vocabulary service."""

    return VocabularyService()

