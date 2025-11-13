"""Backward-compatible re-export of vocabulary helpers for schemas."""
from __future__ import annotations

from bioetl.core.utils.vocab_store import (
    VocabStoreError,
    get_ids as _get_ids,
    load_vocab_store as _load_vocab_store,
)
from bioetl.vocab import (
    VOCAB_STORE_ENV_VAR,
    refresh_vocab_cache as refresh_vocab_store_cache,
    required_vocab_ids,
    vocab_ids,
    vocab_store,
)

load_vocab_store = _load_vocab_store
get_ids = _get_ids

__all__ = [
    "VOCAB_STORE_ENV_VAR",
    "VocabStoreError",
    "load_vocab_store",
    "get_ids",
    "refresh_vocab_store_cache",
    "required_vocab_ids",
    "vocab_ids",
    "vocab_store",
]
