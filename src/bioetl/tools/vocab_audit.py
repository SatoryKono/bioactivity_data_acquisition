"""Аудит словарей ChEMBL против локальных определений."""

from __future__ import annotations

import csv
import hashlib
import os
import subprocess
from collections import Counter
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol, cast

import yaml

from bioetl.core.logger import UnifiedLogger
from bioetl.etl.vocab_store import VocabStoreError, load_vocab_store
from bioetl.tools.chembl_stub import get_offline_new_client

_chembl_new_client: Any | None = None
_chembl_import_error: Exception | None = None
try:  # pragma: no cover - runtime optional dependency
    from chembl_webresource_client.new_client import new_client as _chembl_new_client
except Exception as exc:  # pragma: no cover - offline fallback
    _chembl_new_client = None
    _chembl_import_error = exc
else:  # pragma: no cover - executed when dependency available
    _chembl_import_error = None

__all__ = ["audit_vocabularies", "FieldSpec"]


DEFAULT_OUTPUT = Path("artifacts/vocab_audit.csv")
DEFAULT_META = Path("artifacts/vocab_audit.meta.yaml")
DEFAULT_AGGREGATED = Path("artifacts/chembl_dictionaries.yaml")
LEGACY_AGGREGATED = Path("configs/chembl_dictionaries.yaml")
DEFAULT_DICTIONARY_DIR = Path("configs/dictionaries")
PIPELINE_VERSION = "0.1.0"


class _QueryProtocol(Protocol):
    def only(self, field: str) -> _QueryProtocol:
        ...

    def __iter__(self) -> Iterator[Mapping[str, object]]:
        ...


class _ResourceProtocol(Protocol):
    def filter(self, **filters: Any) -> _QueryProtocol:
        ...


class _ChemblClientProtocol(Protocol):
    activity: _ResourceProtocol
    assay: _ResourceProtocol
    target: _ResourceProtocol
    data_validity_lookup: _ResourceProtocol
    mechanism: _ResourceProtocol


@dataclass(frozen=True)
class FieldSpec:
    dictionary: str
    resource: str
    field: str
    only: str | None = None
    filters: Mapping[str, Any] | None = None


OFFLINE_CLIENT_ENV = "BIOETL_OFFLINE_CHEMBL_CLIENT"


def _is_truthy(value: str | None) -> bool:
    if value is None:
        return False
    normalized = value.strip().lower()
    return normalized in {"1", "true", "yes", "on"}


def _resolve_chembl_client() -> _ChemblClientProtocol:
    use_offline = _is_truthy(os.getenv(OFFLINE_CLIENT_ENV))
    if not use_offline and _chembl_new_client is not None:
        return cast(_ChemblClientProtocol, _chembl_new_client)

    log = UnifiedLogger.get(__name__)
    if _chembl_import_error is not None:
        log.warning(
            "chembl_client.offline_stub_activated",
            reason=str(_chembl_import_error),
        )
    else:
        log.info("chembl_client.offline_stub_forced")
    return cast(_ChemblClientProtocol, get_offline_new_client())


new_client: _ChemblClientProtocol = _resolve_chembl_client()


FIELD_SPECS: tuple[FieldSpec, ...] = (
    FieldSpec(
        dictionary="activity_standard_type",
        resource="activity",
        field="standard_type",
        only="standard_type",
    ),
    FieldSpec(
        dictionary="activity_units",
        resource="activity",
        field="standard_units",
        only="standard_units",
    ),
    FieldSpec(
        dictionary="activity_relation",
        resource="activity",
        field="standard_relation",
        only="standard_relation",
    ),
    FieldSpec(dictionary="assay_type", resource="assay", field="assay_type", only="assay_type"),
    FieldSpec(dictionary="target_type", resource="target", field="target_type", only="target_type"),
    FieldSpec(
        dictionary="data_validity_comment",
        resource="data_validity_lookup",
        field="data_validity_comment",
        only="data_validity_comment",
    ),
    FieldSpec(
        dictionary="action_type", resource="mechanism", field="action_type", only="action_type"
    ),
    FieldSpec(dictionary="bao_format", resource="activity", field="bao_format", only="bao_format"),
)


def _resolve_store_path(store: Path | None) -> Path:
    if store is not None:
        return store.expanduser().resolve()
    aggregate = DEFAULT_AGGREGATED.resolve()
    if aggregate.exists():
        return aggregate
    legacy = LEGACY_AGGREGATED.resolve()
    if legacy.exists():
        return legacy
    return DEFAULT_DICTIONARY_DIR.resolve()


def _load_store(path: Path) -> Mapping[str, object]:
    try:
        store = load_vocab_store(path)
    except VocabStoreError as exc:
        raise RuntimeError(f"Failed to load vocabulary store at {path}: {exc}") from exc
    return cast(Mapping[str, object], store)


def _select_mapping_entries(values: list[object]) -> list[Mapping[str, object]]:
    typed_entries: list[Mapping[str, object]] = []
    for entry in values:
        if isinstance(entry, Mapping):
            typed_entries.append(cast(Mapping[str, object], entry))
    return typed_entries


def _iter_aliases(candidates: list[object]) -> Iterator[str]:
    for alias in candidates:
        if isinstance(alias, str) and alias:
            yield alias


def _dictionary_lookup(block: Mapping[str, object]) -> dict[str, str]:
    values_obj = block.get("values")
    if not isinstance(values_obj, list):
        raise RuntimeError("Dictionary block missing 'values' array")

    mapping: dict[str, str] = {}
    typed_entries = _select_mapping_entries(cast(list[object], values_obj))
    for entry in typed_entries:
        identifier = entry.get("id")
        if not isinstance(identifier, str):
            continue
        status = str(entry.get("status", "active")).strip().lower()
        mapping[identifier] = status
        aliases_obj = entry.get("aliases")
        if isinstance(aliases_obj, list):
            for alias in _iter_aliases(cast(list[object], aliases_obj)):
                mapping.setdefault(alias, "alias")
    return mapping


def _normalise_value(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _classify(status: str | None) -> str:
    if status is None:
        return "new"
    lowered = status.lower()
    if lowered == "active":
        return "ok"
    if lowered == "alias":
        return "alias"
    if lowered == "deprecated":
        return "deprecated"
    return lowered


def _fetch_unique_values(spec: FieldSpec, *, page_size: int, pages: int) -> Counter[str]:
    resource = cast(_ResourceProtocol, getattr(new_client, spec.resource))
    counter: Counter[str] = Counter()
    filters_base = dict(spec.filters or {})

    for page in range(pages):
        filters = dict(filters_base)
        filters["limit"] = page_size
        filters["offset"] = page * page_size
        query = resource.filter(**filters)
        if spec.only:
            query = query.only(spec.only)

        page_count = 0
        for record in query:
            value = _normalise_value(record.get(spec.field))
            if value is None:
                continue
            counter[value] += 1
            page_count += 1
        if page_count < page_size:
            break
    return counter


def _write_csv(rows: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    fieldnames = ["dictionary", "resource", "field", "value", "status", "occurrences"]
    with tmp_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp_path, path)


def _blake2_checksum(path: Path) -> str:
    digest = hashlib.blake2b(digest_size=32)
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(64 * 1024), b""):
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _compute_business_key_hash(rows: list[dict[str, Any]]) -> str:
    digest = hashlib.blake2b(digest_size=32)
    for row in rows:
        payload = f"{row['dictionary']}|{row['value']}|{row['status']}"
        digest.update(payload.encode("utf-8"))
    return digest.hexdigest()


def _git_commit() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"
    return result.stdout.strip()


def _extract_release(store: Mapping[str, object]) -> str | None:
    meta_obj = store.get("meta")
    if isinstance(meta_obj, Mapping):
        meta_mapping = cast(Mapping[str, object], meta_obj)
        release_obj = meta_mapping.get("chembl_release")
        if isinstance(release_obj, str):
            return release_obj

    for block in store.values():
        if not isinstance(block, Mapping):
            continue
        block_mapping = cast(Mapping[str, object], block)
        block_meta = block_mapping.get("meta")
        if not isinstance(block_meta, Mapping):
            continue
        block_meta_mapping = cast(Mapping[str, object], block_meta)
        release_obj = block_meta_mapping.get("chembl_release")
        if isinstance(release_obj, str):
            return release_obj
    return None


@dataclass(frozen=True)
class VocabAuditResult:
    rows: tuple[dict[str, Any], ...]
    output: Path
    meta: Path


def audit_vocabularies(
    store: Path | None = None,
    output: Path | None = None,
    meta: Path | None = None,
    *,
    pages: int = 10,
    page_size: int = 1000,
) -> VocabAuditResult:
    """Аудирует словари и возвращает результаты."""

    UnifiedLogger.configure()
    log = UnifiedLogger.get(__name__)

    resolved_store = _resolve_store_path(store)
    vocab_store = _load_store(resolved_store)

    lookups: dict[str, dict[str, str]] = {}
    skipped: set[str] = set()
    for spec in FIELD_SPECS:
        if spec.dictionary in lookups or spec.dictionary in skipped:
            continue
        block = vocab_store.get(spec.dictionary)
        if not isinstance(block, Mapping):
            log.warning(
                "dictionary_missing",
                dictionary=spec.dictionary,
                store=str(resolved_store),
            )
            skipped.add(spec.dictionary)
            continue
        block_mapping = cast(Mapping[str, object], block)
        lookups[spec.dictionary] = _dictionary_lookup(block_mapping)

    audit_rows: list[dict[str, Any]] = []
    for spec in FIELD_SPECS:
        if spec.dictionary in skipped:
            continue
        counter = _fetch_unique_values(spec, page_size=page_size, pages=pages)
        dictionary_map = lookups[spec.dictionary]
        for value, count in counter.items():
            classification = _classify(dictionary_map.get(value))
            audit_rows.append(
                {
                    "dictionary": spec.dictionary,
                    "resource": spec.resource,
                    "field": spec.field,
                    "value": value,
                    "status": classification,
                    "occurrences": count,
                }
            )

    audit_rows.sort(key=lambda row: (row["dictionary"], row["status"], row["value"]))

    output_path = (output or DEFAULT_OUTPUT).expanduser().resolve()
    meta_path = (meta or DEFAULT_META).expanduser().resolve()

    _write_csv(audit_rows, output_path)

    checksum = _blake2_checksum(output_path)
    business_key_hash = _compute_business_key_hash(audit_rows)
    generated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    release = _extract_release(vocab_store)
    config_payload = f"store={resolved_store}|pages={pages}|page_size={page_size}"
    config_hash = hashlib.blake2b(config_payload.encode("utf-8"), digest_size=32).hexdigest()

    meta_payload = {
        "pipeline_version": PIPELINE_VERSION,
        "git_commit": _git_commit(),
        "config_hash": config_hash,
        "row_count": len(audit_rows),
        "blake2_checksum": checksum,
        "business_key_hash": business_key_hash,
        "generated_at_utc": generated_at,
        "chembl_release": release,
        "vocab_store_path": str(resolved_store),
        "sample": {
            "pages": pages,
            "page_size": page_size,
        },
    }

    meta_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_meta = meta_path.with_suffix(meta_path.suffix + ".tmp")
    with tmp_meta.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(meta_payload, handle, sort_keys=True, allow_unicode=True)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp_meta, meta_path)

    log.info(
        "vocab_audit_completed",
        rows=len(audit_rows),
        output=str(output_path),
        meta=str(meta_path),
    )

    return VocabAuditResult(rows=tuple(audit_rows), output=output_path, meta=meta_path)
