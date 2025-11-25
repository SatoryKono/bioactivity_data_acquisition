from __future__ import annotations

import re
from abc import ABC, abstractmethod
from pathlib import Path


class PathStrategyABC(ABC):
    """Определяет расположение выходных артефактов."""

    @abstractmethod
    def resolve_path(self, dataset: str, run_id: str) -> Path:
        """Возвращает путь/URI для вывода."""


class DeterministicPathStrategy(PathStrategyABC):
    """Детерминированное построение путей вывода.

    Формирует иерархию `<root>/<dataset>/<run_id>/<dataset>.<ext>` с
    нормализацией имени датасета. При одинаковом `run_id` и входных
    параметрах возвращает идентичные пути, что упрощает повторные
    перезапуски пайплайна.
    """

    _SAFE_CHARS = re.compile(r"[^A-Za-z0-9_.-]+")

    def __init__(self, root: str | Path, *, extension: str = ".csv") -> None:
        self.root = Path(root)
        self.extension = extension if extension.startswith(".") else f".{extension}"

    def resolve_path(self, dataset: str, run_id: str) -> Path:
        normalized_dataset = self._normalize(dataset)
        return (
            self.root
            / normalized_dataset
            / run_id
            / f"{normalized_dataset}{self.extension}"
        )

    def resolve_metadata_path(self, dataset: str, run_id: str) -> Path:
        data_path = self.resolve_path(dataset, run_id)
        return data_path.with_suffix(data_path.suffix + ".meta.yaml")

    def _normalize(self, name: str) -> str:
        normalized = self._SAFE_CHARS.sub("_", name.strip())
        return normalized or "dataset"
