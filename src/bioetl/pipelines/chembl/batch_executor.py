from __future__ import annotations

"""Batch execution helper for ChEMBL fetchers."""

from typing import Any, Callable, Mapping, Sequence
import time

import pandas as pd

from bioetl.core.pipeline.unified import BatchExtractionStats, CircuitBreakerOpenError

Fetcher = Callable[[Sequence[str] | None], Any]


def _build_batches(ids: Sequence[str] | None, batch_size: int) -> list[Sequence[str] | None]:
    if not ids:
        return [None]
    safe_batch = max(1, min(batch_size, 25))
    return [ids[i : i + safe_batch] for i in range(0, len(ids), safe_batch)]


def execute_batch_extraction(
    fetcher: Fetcher, *, ids: Sequence[str] | None, batch_size: int = 25
) -> tuple[pd.DataFrame, BatchExtractionStats]:
    """Run fetcher in batches aggregating stats and handling circuit breaker."""

    batches = _build_batches(ids, batch_size)

    start = time.perf_counter()
    frames: list[pd.DataFrame] = []
    api_calls = cache_hits = success = fallback = errors = 0

    for batch in batches:
        try:
            result = fetcher(batch)
            meta: Mapping[str, Any] | None = None
            batch_df: pd.DataFrame
            if isinstance(result, tuple) and len(result) == 2 and isinstance(result[1], Mapping):
                batch_df = pd.DataFrame(result[0]) if not isinstance(result[0], pd.DataFrame) else result[0]
                meta = result[1]
            else:
                batch_df = pd.DataFrame(result)

            frames.append(batch_df)
            meta = meta or {}
            api_calls += int(meta.get("api_calls", 0 if meta.get("cache_hit") else 1))
            cache_hits += int(meta.get("cache_hit", False)) * max(len(batch_df), 1)
            fallback += int(meta.get("fallback", 0))
            success += int(batch_df.shape[0])
        except CircuitBreakerOpenError:
            errors += 1
            break
        except Exception:
            errors += 1
            continue

    dataframe = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
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
