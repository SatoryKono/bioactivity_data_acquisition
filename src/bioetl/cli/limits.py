"""Shared helpers for applying runtime sample limits to pipelines."""

from __future__ import annotations

from types import MethodType
from typing import Any

import pandas as pd

from bioetl.core.logger import UnifiedLogger
from bioetl.pipelines.base import PipelineBase


def _resolve_logger(pipeline: PipelineBase) -> Any:
    """Return a structured logger bound to the pipeline name."""

    pipeline_name: str | None = None
    config = getattr(pipeline, "config", None)
    if config is not None:
        pipeline_config = getattr(config, "pipeline", None)
        pipeline_name = getattr(pipeline_config, "name", None)

    if not pipeline_name:
        pipeline_name = pipeline.__class__.__name__.lower()

    return UnifiedLogger.get(f"cli.{pipeline_name}")


def apply_sample_limit(pipeline: PipelineBase, limit: int | None) -> None:
    """Apply an in-memory sample limit to the pipeline extract method.

    Parameters
    ----------
    pipeline:
        Instantiated pipeline whose ``extract`` method should be capped.
    limit:
        Positive integer indicating how many rows should be returned. ``None``
        disables the limiter.
    """

    if limit is None:
        return

    runtime_options = getattr(pipeline, "runtime_options", None)
    if isinstance(runtime_options, dict):
        runtime_options["limit"] = limit
        runtime_options.setdefault("sample", limit)

    original_extract = getattr(pipeline, "extract", None)
    if not callable(original_extract):
        return

    logger = _resolve_logger(pipeline)

    def limited_extract(self: PipelineBase, *args: Any, **kwargs: Any) -> pd.DataFrame:
        df = original_extract(*args, **kwargs)
        logger.info(
            "applying_sample_limit",
            limit=limit,
            original_rows=len(df) if hasattr(df, "__len__") else None,
        )
        return df.head(limit) if hasattr(df, "head") else df

    pipeline.extract = MethodType(limited_extract, pipeline)  # type: ignore[method-assign]

