"""Сборка агрегированного словаря ChEMBL."""

from __future__ import annotations

import os
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

import yaml

from bioetl.core.logger import UnifiedLogger
from bioetl.core.log_events import LogEvents
from bioetl.core.utils.vocab_store import VocabStoreError, clear_vocab_store_cache, load_vocab_store

__all__ = ["build_vocab_store"]


def _utc_timestamp() -> str:
    now = datetime.now(timezone.utc)
    return now.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _atomic_write_yaml(payload: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(dict(payload), handle, sort_keys=False, allow_unicode=True)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp_path, path)


def _extract_release(
    meta: Mapping[str, Any] | None,
    *,
    name: str,
    current: str | None,
) -> str | None:
    if meta is None:
        return current
    if not isinstance(meta, Mapping):
        raise VocabStoreError(f"Dictionary '{name}' meta section must be a mapping")
    release = meta.get("chembl_release")
    if release is None:
        return current
    if not isinstance(release, str):
        raise VocabStoreError(
            f"Dictionary '{name}' meta.chembl_release must be a string when present"
        )
    if current is None:
        return release
    if current != release:
        raise VocabStoreError(
            "Inconsistent chembl_release across dictionaries: "
            f"'{current}' vs '{release}' (from {name})"
        )
    return current


def build_vocab_store(
    src: Path,
    output: Path,
) -> Path:
    """Агрегирует отдельные словари в единый YAML-файл."""

    UnifiedLogger.configure()
    log = UnifiedLogger.get(__name__)

    resolved_src = src.expanduser().resolve()
    resolved_output = output.expanduser().resolve()

    clear_vocab_store_cache()
    store = load_vocab_store(resolved_src)
    if not store:
        raise VocabStoreError(f"No dictionaries found in {resolved_src}")

    chembl_release: str | None = None
    dictionary_names = sorted(key for key in store.keys() if key != "meta")
    aggregated: dict[str, Any] = {}

    for name in dictionary_names:
        block_raw = store[name]
        if not isinstance(block_raw, Mapping):
            raise VocabStoreError(f"Dictionary '{name}' payload must be a mapping")

        block = cast(Mapping[str, Any], block_raw)
        meta_section_raw: Any = block.get("meta")
        if meta_section_raw is None:
            meta_section: Mapping[str, Any] | None = None
        elif isinstance(meta_section_raw, Mapping):
            meta_section = cast(Mapping[str, Any], meta_section_raw)
        else:
            raise VocabStoreError(
                f"Dictionary '{name}' meta section must be a mapping when present"
            )

        chembl_release = _extract_release(
            meta_section,
            name=name,
            current=chembl_release,
        )
        aggregated[name] = dict(block)

    if chembl_release is None:
        raise VocabStoreError("chembl_release metadata is missing across dictionaries")

    aggregated_with_meta: dict[str, Any] = {
        "meta": {
            "built_at": _utc_timestamp(),
            "chembl_release": chembl_release,
        }
    }
    aggregated_with_meta.update(aggregated)

    _atomic_write_yaml(aggregated_with_meta, resolved_output)
    log.info(LogEvents.VOCAB_STORE_BUILT, source=str(resolved_src), output=str(resolved_output))
    return resolved_output
