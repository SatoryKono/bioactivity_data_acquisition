from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Iterable, Mapping, Sequence

import pandas as pd

from ..pipeline.dto import WriteResult
from .metadata import MetadataWriterABC
from .path_strategy import PathStrategyABC


class WriterABC(ABC):
    """Пишет данные в целевое хранилище."""

    @abstractmethod
    def write(self, dataset: str, records: object) -> WriteResult:
        """Сохраняет данные и возвращает результат записи."""


class AtomicFileWriter(WriterABC):
    """Атомарная запись CSV/Parquet с сортировкой и UTF-8."""

    def __init__(
        self,
        run_id: str,
        path_strategy: PathStrategyABC,
        *,
        metadata_writer: MetadataWriterABC | None = None,
        sort_columns: Sequence[str] | None = None,
    ) -> None:
        self.run_id = run_id
        self._path_strategy = path_strategy
        self._metadata_writer = metadata_writer
        self._sort_columns = list(sort_columns) if sort_columns is not None else None

    def write(self, dataset: str, records: object) -> WriteResult:
        if not isinstance(records, pd.DataFrame):
            raise TypeError("records must be a pandas DataFrame")

        target_path = Path(self._path_strategy.resolve_path(dataset, self.run_id))
        target_path.parent.mkdir(parents=True, exist_ok=True)

        started_at = datetime.now(timezone.utc)
        df_to_write = self._prepare_dataframe(records)
        tmp_path = self._write_tmp_file(target_path, df_to_write)

        metadata: Mapping[str, object] | None = None
        try:
            data_hash = _hash_file(tmp_path)
            tmp_path.replace(target_path)

            metadata = {
                "run_id": self.run_id,
                "dataset": dataset,
                "output_path": str(target_path),
                "rows": int(df_to_write.shape[0]),
                "columns": list(df_to_write.columns),
                "column_hash": _hash_columns(df_to_write.columns),
                "data_sha256": data_hash,
            }

            if self._metadata_writer is not None:
                metadata = self._metadata_writer.write_metadata(dataset, metadata).metadata
        finally:
            if tmp_path.exists() and not tmp_path.samefile(target_path):
                tmp_path.unlink(missing_ok=True)

        finished_at = datetime.now(timezone.utc)
        return WriteResult(
            run_id=self.run_id,
            output_uri=str(target_path),
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=(finished_at - started_at).total_seconds(),
            rows_written=df_to_write.shape[0],
            metadata=metadata or {},
        )

    def _prepare_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        columns = self._sort_columns or sorted(df.columns.tolist())
        normalized_df = df.loc[:, columns]
        return normalized_df.sort_values(by=columns).reset_index(drop=True)

    def _write_tmp_file(self, target_path: Path, df: pd.DataFrame) -> Path:
        suffix = target_path.suffix.lower()
        with NamedTemporaryFile(delete=False, dir=target_path.parent, suffix=suffix) as tmp:
            tmp_path = Path(tmp.name)
        if suffix == ".csv":
            df.to_csv(tmp_path, index=False, encoding="utf-8")
        elif suffix in {".parquet", ".pq"}:
            df.to_parquet(tmp_path, index=False)
        else:
            tmp_path.unlink(missing_ok=True)
            raise ValueError(f"Unsupported output format: {target_path.suffix}")
        return tmp_path


def _hash_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as fp:
        for chunk in iter(lambda: fp.read(8192), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _hash_columns(columns: Iterable[str]) -> str:
    digest = sha256()
    digest.update("|".join(columns).encode())
    return digest.hexdigest()
