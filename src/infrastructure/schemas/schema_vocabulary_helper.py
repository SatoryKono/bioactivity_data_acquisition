"""Backward-compatible wrappers around the vocabulary service API."""

from __future__ import annotations

from common.core_utils.vocab_store import VocabStoreError, get_ids as _get_ids, load_vocab_store
from domain.vocab.service import (
    VOCAB_STORE_ENV_VAR,
    refresh_vocab_cache as refresh_vocab_store_cache,
    required_vocab_ids,
)

__all__ = [
    "VOCAB_STORE_ENV_VAR",
    "VocabStoreError",
    "load_vocab_store",
    "_get_ids",
    "refresh_vocab_store_cache",
    "required_vocab_ids",
]
