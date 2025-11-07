"""Utilities for loading and querying standardized vocabulary dictionaries.

The vocab store may be represented either by a directory of YAML files where each
file contains a single dictionary definition, or by an aggregated YAML file that
combines all dictionaries into a single document. This module provides a thin
abstraction for loading the store in either form with deterministic behaviour and
basic integrity checks to ensure the dictionaries remain well-formed.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from functools import lru_cache
from pathlib import Path
from typing import Any, Final, TypeGuard, cast

import yaml

VALID_ENTRY_STATUSES: Final[set[str]] = {"active", "alias", "deprecated"}
DEFAULT_ALLOWED_STATUSES: Final[set[str]] = {"active", "alias"}


class VocabStoreError(RuntimeError):
    """Raised when the vocabulary store or a dictionary block fails validation."""


def _ensure_mapping(value: Any, *, context: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise VocabStoreError(f"Expected mapping for {context}, got {type(value)!r}")

    keys_view = cast(Iterable[Any], value.keys())

    for key_obj in keys_view:
        if not isinstance(key_obj, str):
            raise VocabStoreError(
                f"Expected string keys for {context}, got key of type {type(key_obj)!r}"
            )

    return cast(Mapping[str, Any], value)


def _is_list_of_any(value: Any) -> TypeGuard[list[Any]]:
    return isinstance(value, list)


def _ensure_list(value: Any, *, context: str) -> list[Any]:
    if not _is_list_of_any(value):
        raise VocabStoreError(f"Expected list for {context}, got {type(value)!r}")

    copied_list: list[Any] = list(value)
    return copied_list


def _validate_block(name: str, block: Mapping[str, Any]) -> None:
    values_list = _ensure_list(block.get("values"), context=f"{name}.values")
    if not values_list:
        raise VocabStoreError(f"Dictionary '{name}' must contain at least one value")

    seen_ids: set[str] = set()
    for index, entry_raw in enumerate(values_list):
        entry = _ensure_mapping(entry_raw, context=f"{name}.values[{index}]")
        entry_id = entry.get("id")
        if not isinstance(entry_id, str) or not entry_id.strip():
            raise VocabStoreError(
                f"Dictionary '{name}' contains an entry without a valid 'id' field"
            )

        normalized_id = entry_id.strip()
        if normalized_id in seen_ids:
            raise VocabStoreError(
                f"Duplicate id '{normalized_id}' detected in dictionary '{name}'"
            )
        seen_ids.add(normalized_id)

        status = entry.get("status", "active")
        if not isinstance(status, str):
            raise VocabStoreError(
                f"Dictionary '{name}' entry '{normalized_id}' has non-string status"
            )
        normalized_status = status.strip().lower()
        if normalized_status not in VALID_ENTRY_STATUSES:
            raise VocabStoreError(
                f"Dictionary '{name}' entry '{normalized_id}' has invalid status '{status}'"
            )


def _load_yaml(path: Path) -> Mapping[str, Any]:
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise VocabStoreError(f"Vocabulary file not found: {path}") from exc
    except yaml.YAMLError as exc:  # pragma: no cover - re-raising with context
        raise VocabStoreError(f"Failed to parse vocabulary file {path}: {exc}") from exc

    return _ensure_mapping(payload, context=f"YAML root from {path}")


def _normalize_path(path: str | Path) -> Path:
    return Path(path).expanduser().resolve()


@lru_cache(maxsize=8)
def _load_vocab_store_cached(resolved_path: str) -> dict[str, Any]:
    path = Path(resolved_path)
    if path.is_dir():
        return _load_from_directory(path)
    if path.is_file():
        return _load_from_file(path)
    raise VocabStoreError(f"Vocabulary store path does not exist: {path}")


def _load_from_directory(directory: Path) -> dict[str, Any]:
    store: dict[str, Any] = {}
    for yaml_file in sorted(directory.glob("*.yaml")):
        block = _load_yaml(yaml_file)
        name = yaml_file.stem
        _validate_block(name, block)
        store[name] = block
    return store


def _load_from_file(file_path: Path) -> dict[str, Any]:
    store = dict(_load_yaml(file_path))
    for name, block_raw in store.items():
        if name == "meta":
            continue
        block = _ensure_mapping(block_raw, context=f"{name} block")
        _validate_block(name, block)
    return store


def clear_vocab_store_cache() -> None:
    """Invalidate the internal cache used by :func:`load_vocab_store`."""

    _load_vocab_store_cached.cache_clear()


def load_vocab_store(path: str | Path) -> dict[str, Any]:
    """Load a vocabulary store from a directory of YAML files or an aggregate file.

    Parameters
    ----------
    path:
        Path to the directory containing dictionary YAML files or to an aggregated
        YAML file.

    Returns
    -------
    dict[str, Any]
        Mapping of dictionary name to its parsed payload. When an aggregated file
        is used, the special ``meta`` key is preserved as-is.
    """

    normalized = _normalize_path(path)
    return _load_vocab_store_cached(str(normalized))


def get_ids(
    store: Mapping[str, Any],
    name: str,
    *,
    allowed_statuses: Iterable[str] | None = None,
) -> set[str]:
    """Return the set of entry identifiers for the requested dictionary block.

    Only entries whose ``status`` is within ``allowed_statuses`` are returned. By
    default, this includes entries marked as ``active`` or ``alias``. The check is
    case-insensitive and treats whitespace as insignificant.
    """

    try:
        block_raw = store[name]
    except KeyError as exc:  # pragma: no cover - defensive guard
        raise VocabStoreError(f"Dictionary '{name}' not found in vocabulary store") from exc

    block = _ensure_mapping(block_raw, context=f"dictionary block '{name}'")
    values = _ensure_list(block.get("values"), context=f"{name}.values")

    if allowed_statuses is None:
        allowed_statuses_set = DEFAULT_ALLOWED_STATUSES
    else:
        allowed_statuses_set = {status.strip().lower() for status in allowed_statuses}

    result: set[str] = set()
    for index, entry_raw in enumerate(values):
        entry = _ensure_mapping(entry_raw, context=f"{name}.values[{index}]")
        entry_id = entry.get("id")
        if not isinstance(entry_id, str):
            raise VocabStoreError(
                f"Dictionary '{name}' entry at position {index} is missing a string 'id'"
            )

        status_raw = entry.get("status", "active")
        if not isinstance(status_raw, str):
            raise VocabStoreError(
                f"Dictionary '{name}' entry '{entry_id}' has non-string status"
            )

        status = status_raw.strip().lower()
        if status in allowed_statuses_set:
            result.add(entry_id.strip())

    return result


__all__ = [
    "DEFAULT_ALLOWED_STATUSES",
    "VALID_ENTRY_STATUSES",
    "VocabStoreError",
    "clear_vocab_store_cache",
    "get_ids",
    "load_vocab_store",
]

