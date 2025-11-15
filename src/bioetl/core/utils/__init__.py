"""Helper utilities for the BioETL core package."""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

from .iterables import is_non_string_iterable
from .vocab_store import (
    DEFAULT_ALLOWED_STATUSES,
    VALID_ENTRY_STATUSES,
    VocabStoreError,
    clear_vocab_store_cache,
    get_ids,
    load_vocab_store,
)

if TYPE_CHECKING:
    from .molecule_map import join_activity_with_molecule

__all__ = [
    "DEFAULT_ALLOWED_STATUSES",
    "VALID_ENTRY_STATUSES",
    "VocabStoreError",
    "clear_vocab_store_cache",
    "get_ids",
    "is_non_string_iterable",
    "join_activity_with_molecule",
    "load_vocab_store",
]


def __getattr__(name: str) -> Any:
    if name == "join_activity_with_molecule":
        from .molecule_map import join_activity_with_molecule as join_fn

        return join_fn
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
