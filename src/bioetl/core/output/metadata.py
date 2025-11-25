from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Mapping

import yaml

from ..pipeline.dto import WriteResult
from .path_strategy import DeterministicPathStrategy, PathStrategyABC


class MetadataWriterABC(ABC):
    """Сохраняет вспомогательные метаданные для аудита."""

    @abstractmethod
    def write_metadata(self, dataset: str, metadata: Mapping[str, object]) -> WriteResult:
        """Записывает метаданные о процессе загрузки."""


class YamlMetadataWriter(MetadataWriterABC):
    """Запись метаданных запуска в `meta.yaml` рядом с данными."""

    def __init__(self, run_id: str, path_strategy: PathStrategyABC | None = None) -> None:
        self.run_id = run_id
        self._path_strategy = path_strategy or DeterministicPathStrategy("./output")

    def write_metadata(self, dataset: str, metadata: Mapping[str, object]) -> WriteResult:
        base_metadata = {"run_id": self.run_id, **dict(metadata)}
        meta_path = self._resolve_metadata_path(dataset)
        meta_path.parent.mkdir(parents=True, exist_ok=True)

        started_at = datetime.now(timezone.utc)
        with NamedTemporaryFile(delete=False, dir=meta_path.parent, suffix=meta_path.suffix) as tmp:
            tmp_path = Path(tmp.name)
        try:
            dump = yaml.safe_dump(base_metadata, sort_keys=True, allow_unicode=True)
            tmp_path.write_text(dump, encoding="utf-8")
            tmp_hash = _hash_file(tmp_path)
            tmp_path.replace(meta_path)
        finally:
            if tmp_path.exists() and not tmp_path.samefile(meta_path):
                tmp_path.unlink(missing_ok=True)
        finished_at = datetime.now(timezone.utc)
        return WriteResult(
            run_id=self.run_id,
            output_uri=str(meta_path),
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=(finished_at - started_at).total_seconds(),
            metadata={**base_metadata, "metadata_sha256": tmp_hash},
        )

    def _resolve_metadata_path(self, dataset: str) -> Path:
        if isinstance(self._path_strategy, DeterministicPathStrategy):
            return self._path_strategy.resolve_metadata_path(dataset, self.run_id)
        data_path = self._path_strategy.resolve_path(dataset, self.run_id)
        path = Path(data_path)
        return path.with_suffix(path.suffix + ".meta.yaml")


def _hash_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as fp:
        for chunk in iter(lambda: fp.read(8192), b""):
            digest.update(chunk)
    return digest.hexdigest()
