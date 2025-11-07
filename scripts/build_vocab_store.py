"""Build an aggregated ChEMBL vocabulary store from individual dictionary files."""

from __future__ import annotations

import os
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

import typer
import yaml

from bioetl.etl.vocab_store import VocabStoreError, clear_vocab_store_cache, load_vocab_store

app = typer.Typer(add_completion=False, help=__doc__)


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
    meta: Mapping[str, Any] | None, *, name: str, current: str | None
) -> str | None:
    if meta is None:
        return current
    if not isinstance(meta, dict):
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


@app.command()
def main(
    src: Path = typer.Option(
        Path("configs/dictionaries"),
        help="Directory with individual dictionary YAML files.",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
    ),
    output: Path = typer.Option(
        Path("configs/chembl_dictionaries.yaml"),
        help="Destination path for the aggregated vocabulary store.",
        file_okay=True,
        dir_okay=False,
        writable=True,
    ),
) -> None:
    """Aggregate individual dictionary YAML files into a single vocab store."""

    resolved_src = src.expanduser().resolve()
    resolved_output = output.expanduser().resolve()

    try:
        clear_vocab_store_cache()
        store = load_vocab_store(resolved_src)
        if not store:
            raise VocabStoreError(f"No dictionaries found in {resolved_src}")

        chembl_release: str | None = None
        dictionary_names = sorted(key for key in store.keys() if key != "meta")
        aggregated: dict[str, Any] = {}

        for name in dictionary_names:
            block_raw = store[name]
            if not isinstance(block_raw, dict):
                raise VocabStoreError(f"Dictionary '{name}' payload must be a mapping")

            block = cast(dict[str, Any], block_raw)

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
            aggregated[name] = block

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
        typer.echo(f"Aggregated vocab store written to {resolved_output}")
    except VocabStoreError as exc:
        typer.secho(str(exc), err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1) from exc


if __name__ == "__main__":
    app()

