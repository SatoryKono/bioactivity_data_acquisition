from __future__ import annotations

import hashlib
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd
import yaml


HASH_ALGORITHM = "sha256"


def _ensure_bytes(value: Any) -> bytes:
    if value is None:
        return b""
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    return str(value).encode("utf-8")


def hash_business_key(value: str) -> str:
    digest = hashlib.new(HASH_ALGORITHM)
    digest.update(_ensure_bytes(value))
    return digest.hexdigest()


def hash_row(values: Iterable[Any]) -> str:
    digest = hashlib.new(HASH_ALGORITHM)
    for value in values:
        digest.update(_ensure_bytes(value))
    return digest.hexdigest()


def compute_file_hash(path: Path) -> str:
    digest = hashlib.new(HASH_ALGORITHM)
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_meta_yaml(
    df: pd.DataFrame,
    schema_version: str,
    config_hash: str,
    pipeline_version: str,
    chembl_release: str,
    file_hashes: Mapping[str, str],
    row_count: int,
) -> dict[str, Any]:
    return {
        "schema_version": schema_version,
        "config_hash": config_hash,
        "pipeline_version": pipeline_version,
        "chembl_release": chembl_release,
        "file_hashes": dict(file_hashes),
        "row_count": int(row_count),
        "columns": list(df.columns),
    }


class AtomicWriter:
    """Пишет артефакты в run_id-scoped каталог и переименовывает атомарно."""

    def __init__(self, base_dir: Path, run_id: str) -> None:
        self.base_dir = base_dir
        self.run_id = run_id
        self._temp_dir = base_dir / f".{run_id}.tmp"
        self._final_dir = base_dir / run_id

    @property
    def temp_dir(self) -> Path:
        return self._temp_dir

    @property
    def final_dir(self) -> Path:
        return self._final_dir

    def prepare(self) -> Path:
        self._temp_dir.mkdir(parents=True, exist_ok=True)
        return self._temp_dir

    def write_bytes(self, relative_path: str | Path, data: bytes) -> Path:
        target = self._temp_dir / Path(relative_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        return target

    def write_text(self, relative_path: str | Path, data: str) -> Path:
        return self.write_bytes(relative_path, data.encode("utf-8"))

    def commit(self) -> Path:
        if not self._temp_dir.exists():
            msg = "Temporary directory does not exist; call prepare() first"
            raise RuntimeError(msg)
        if self._final_dir.exists():
            shutil.rmtree(self._final_dir)
        self._temp_dir.replace(self._final_dir)
        return self._final_dir

    def abort(self) -> None:
        if self._temp_dir.exists():
            shutil.rmtree(self._temp_dir)

    def __enter__(self) -> "AtomicWriter":
        self.prepare()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        if exc is not None:
            self.abort()
        elif self._temp_dir.exists():
            self.commit()


@dataclass
class UnifiedOutputWriter:
    base_dir: Path
    run_id: str
    schema_version: str
    config_hash: str
    pipeline_version: str
    chembl_release: str

    def write_dataframe(self, df: pd.DataFrame, filename: str = "data.csv") -> Path:
        with AtomicWriter(self.base_dir, self.run_id) as writer:
            csv_path = writer.temp_dir / filename
            df.to_csv(csv_path, index=False)
            file_hashes = {filename: compute_file_hash(csv_path)}
            meta = build_meta_yaml(
                df,
                self.schema_version,
                self.config_hash,
                self.pipeline_version,
                self.chembl_release,
                file_hashes,
                row_count=len(df),
            )
            writer.write_text("meta.yaml", yaml.safe_dump(meta, allow_unicode=True))
            return writer.commit()
