"""IO-примитивы для Chembl пайплайнов."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import Any, Callable

import pandas as pd


class ChemblIO:
    """Упрощённые операции ввода-вывода для пайплайна."""

    def __init__(self, chunk_size: int = 1000):
        self.chunk_size = chunk_size

    def chunked_fetch(
        self,
        source: (
            Iterable[dict[str, Any]] | Callable[[], Iterable[dict[str, Any]]]
        ),
    ) -> Iterator[list[dict[str, Any]]]:
        """Разбить входной источник на чанки фиксированного размера."""

        iterable = source() if callable(source) else source
        batch: list[dict[str, Any]] = []
        for item in iterable:
            batch.append(item)
            if len(batch) >= self.chunk_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def write_dataframe(
        self, df: pd.DataFrame, writer: Callable[[pd.DataFrame], Any]
    ) -> Any:
        """Записать ``DataFrame`` через переданный writer-коллбек."""

        return writer(df)
