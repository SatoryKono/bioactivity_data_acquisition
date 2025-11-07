"""Audit ChEMBL API vocabularies against local dictionary definitions."""

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

import typer
import yaml
from chembl_webresource_client.new_client import new_client

from bioetl.etl.vocab_store import VocabStoreError, load_vocab_store


class _QueryProtocol(Protocol):
    def only(self, field: str) -> _QueryProtocol:
        ...

    def __iter__(self) -> Iterator[Mapping[str, object]]:
        ...


class _ResourceProtocol(Protocol):
    def filter(self, **filters: Any) -> _QueryProtocol:
        ...


APP = typer.Typer(add_completion=False, help=__doc__)

DEFAULT_OUTPUT = Path("reports/vocab_audit.csv")
DEFAULT_META = Path("reports/vocab_audit.meta.yaml")
DEFAULT_AGGREGATED = Path("configs/chembl_dictionaries.yaml")
DEFAULT_DICTIONARY_DIR = Path("configs/dictionaries")
PIPELINE_VERSION = "0.1.0"


@dataclass(frozen=True)
class FieldSpec:
    dictionary: str
    resource: str
    field: str
    only: str | None = None
    filters: Mapping[str, Any] | None = None


FIELD_SPECS: tuple[FieldSpec, ...] = (
    FieldSpec(dictionary="activity_standard_type", resource="activity", field="standard_type", only="standard_type"),
    FieldSpec(dictionary="activity_units", resource="activity", field="standard_units", only="standard_units"),
    FieldSpec(dictionary="activity_relation", resource="activity", field="standard_relation", only="standard_relation"),
    FieldSpec(dictionary="assay_type", resource="assay", field="assay_type", only="assay_type"),
    FieldSpec(dictionary="target_type", resource="target", field="target_type", only="target_type"),
    FieldSpec(dictionary="data_validity_comment", resource="data_validity_lookup", field="data_validity_comment", only="data_validity_comment"),
    FieldSpec(dictionary="action_type", resource="mechanism", field="action_type", only="action_type"),
    FieldSpec(dictionary="bao_format", resource="activity", field="bao_format", only="bao_format"),
)


def _resolve_store_path(store: Path | None) -> Path:
    if store is not None:
        return store.expanduser().resolve()
    aggregate = DEFAULT_AGGREGATED.resolve()
    if aggregate.exists():
        return aggregate
    return DEFAULT_DICTIONARY_DIR.resolve()


def _load_store(path: Path) -> Mapping[str, object]:
    try:
        store = load_vocab_store(path)
    except VocabStoreError as exc:  # pragma: no cover - configuration error
        raise typer.BadParameter(f"Failed to load vocabulary store at {path}: {exc}") from exc
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
        raise typer.BadParameter("Dictionary block missing 'values' array")

    mapping: dict[str, str] = {}
    typed_entries = _select_mapping_entries(cast(list[object], values_obj))
    for entry_mapping in typed_entries:
        identifier = entry_mapping.get("id")
        if not isinstance(identifier, str):
            continue
        status = str(entry_mapping.get("status", "active")).strip().lower()
        mapping[identifier] = status
        aliases_obj = entry_mapping.get("aliases")
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
    except (subprocess.CalledProcessError, FileNotFoundError):  # pragma: no cover - optional
        return "unknown"
    return result.stdout.strip()


def _extract_release(store: Mapping[str, object]) -> str | None:
    meta = store.get("meta")
    if isinstance(meta, Mapping):
        meta_mapping = cast(Mapping[str, object], meta)
        release = meta_mapping.get("chembl_release")
        if isinstance(release, str):
            return release
    for block in store.values():
        if isinstance(block, Mapping):
            block_mapping = cast(Mapping[str, object], block)
            block_meta = block_mapping.get("meta")
            if isinstance(block_meta, Mapping):
                block_meta_mapping = cast(Mapping[str, object], block_meta)
                release = block_meta_mapping.get("chembl_release")
                if isinstance(release, str):
                    return release
    return None


@APP.command()
def main(
    store: Path | None = typer.Option(None, help="Path to dictionaries directory or aggregate YAML."),
    output: Path = typer.Option(DEFAULT_OUTPUT, help="Path for the audit CSV report."),
    meta: Path = typer.Option(DEFAULT_META, help="Path for the audit metadata YAML."),
    pages: int = typer.Option(10, min=1, help="Number of pages to sample per field."),
    page_size: int = typer.Option(1000, min=10, help="Page size for ChEMBL API sampling."),
) -> None:
    """Compare sampled ChEMBL values with local vocabularies and emit a report."""

    resolved_store = _resolve_store_path(store)
    vocab_store = _load_store(resolved_store)

    lookups: dict[str, dict[str, str]] = {}
    skipped: set[str] = set()
    for spec in FIELD_SPECS:
        if spec.dictionary in lookups or spec.dictionary in skipped:
            continue
        block = vocab_store.get(spec.dictionary)
        if not isinstance(block, Mapping):
            typer.secho(
                f"Dictionary '{spec.dictionary}' not found in store {resolved_store}; skipping related fields.",
                fg=typer.colors.YELLOW,
                err=True,
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
    _write_csv(audit_rows, output.resolve())

    checksum = _blake2_checksum(output.resolve())
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

    meta.parent.mkdir(parents=True, exist_ok=True)
    tmp_meta = meta.with_suffix(meta.suffix + ".tmp")
    with tmp_meta.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(meta_payload, handle, sort_keys=True, allow_unicode=True)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp_meta, meta)

    typer.echo(f"Vocabulary audit written to {output.resolve()} (rows={len(audit_rows)})")


if __name__ == "__main__":
    APP()

