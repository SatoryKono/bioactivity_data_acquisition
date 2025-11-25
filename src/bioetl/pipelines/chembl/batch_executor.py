from __future__ import annotations

"""Утилиты для батчевой выборки данных ChEMBL."""

import time
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence

import pandas as pd

from bioetl.core.pipeline.unified import (
    BatchExtractionStats,
    CircuitBreakerOpenError,
)


@dataclass(slots=True)
class ChemblBatchExecutor:
    """Оркеструет вызовы fetcher по батчам и собирает статистику."""

    batch_size: int = 25

    def run(
        self,
        fetcher: Callable[[Sequence[str] | None], Any],
        ids: Sequence[str] | None,
    ) -> tuple[pd.DataFrame, BatchExtractionStats]:
        batches = self._build_batches(ids)
        start = time.perf_counter()

        frames: list[pd.DataFrame] = []
        api_calls = cache_hits = success = fallback = errors = 0

        for batch in batches:
            try:
                result = fetcher(batch)
            except CircuitBreakerOpenError:
                errors += 1
                break
            except Exception:
                errors += 1
                continue

            meta: Mapping[str, Any] | None = None
            batch_df: pd.DataFrame
            if (
                isinstance(result, tuple)
                and len(result) == 2
                and isinstance(result[1], Mapping)
            ):
                if isinstance(result[0], pd.DataFrame):
                    batch_df = result[0]
                else:
                    batch_df = pd.DataFrame(result[0])
                meta = result[1]
            else:
                if isinstance(result, pd.DataFrame):
                    batch_df = result
                else:
                    batch_df = pd.DataFrame(result)

            frames.append(batch_df)
            meta = meta or {}
            api_calls += int(
                meta.get("api_calls", 0 if meta.get("cache_hit") else 1),
            )
            cache_hits += int(meta.get("cache_hit", False)) * max(
                batch_df.shape[0],
                1,
            )
            fallback += int(meta.get("fallback", 0))
            success += int(batch_df.shape[0])

        dataframe = (
            pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        )
        duration = time.perf_counter() - start
        stats = BatchExtractionStats(
            rows=int(dataframe.shape[0]),
            api_calls=api_calls,
            cache_hits=cache_hits,
            success_count=success,
            fallback_count=fallback,
            error_count=errors,
            duration_seconds=duration,
        )
        return dataframe, stats

    def _build_batches(
        self,
        ids: Sequence[str] | None,
    ) -> list[Sequence[str] | None]:
        effective_size = int(self.batch_size) if self.batch_size else 25
        effective_size = max(1, min(effective_size, 25))
        sanitized_ids = list(ids) if ids else []

        batches = [
            sanitized_ids[i : i + effective_size]
            for i in range(0, len(sanitized_ids), effective_size)
        ]
        if not batches:
            batches = [None]
        return batches


__all__ = ["ChemblBatchExecutor"]
