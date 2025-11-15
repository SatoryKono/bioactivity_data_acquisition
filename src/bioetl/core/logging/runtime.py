"""Runtime helpers for structured pipeline logging.

This module provides a thin abstraction on top of :class:`UnifiedLogger`
so that pipeline stages can consistently bind contextual information such as
``pipeline``, ``run_id`` and ``stage``.  The helpers centralise how the
structured logger is retrieved and how additional metadata (like filesystem
paths) is attached to each log record.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from structlog.stdlib import BoundLogger

from .logger import UnifiedLogger

__all__ = [
    "bind_pipeline_context",
    "get_pipeline_logger",
    "pipeline_stage",
]

_DEFAULT_LOGGER_NAME = "bioetl.runtime"


def _normalise_path(path: Path | str | None) -> str | None:
    if path is None:
        return None
    if isinstance(path, Path):
        return str(path)
    return str(Path(path))


def get_pipeline_logger(
    *,
    pipeline: str,
    run_id: str | None = None,
    dataset: str | None = None,
    stage: str | None = None,
    component: str | None = None,
    path: Path | str | None = None,
    logger_name: str | None = None,
    **extra: Any,
) -> BoundLogger:
    """Return a bound logger enriched with pipeline metadata."""

    resolved_component = component
    if resolved_component is None and stage:
        resolved_component = f"{pipeline}.{stage}"

    bound_context: dict[str, Any] = {
        "pipeline": pipeline,
        "dataset": dataset or pipeline,
    }
    if run_id is not None:
        bound_context["run_id"] = run_id
    if stage is not None:
        bound_context["stage"] = stage
    if resolved_component is not None:
        bound_context["component"] = resolved_component
    normalised_path = _normalise_path(path)
    if normalised_path is not None:
        bound_context["path"] = normalised_path
    if extra:
        bound_context.update(extra)

    logger = UnifiedLogger.get(logger_name or _DEFAULT_LOGGER_NAME)
    return logger.bind(**bound_context)


def bind_pipeline_context(
    *,
    pipeline: str | None = None,
    run_id: str | None = None,
    dataset: str | None = None,
    component: str | None = None,
    stage: str | None = None,
    trace_id: str | None = None,
    span_id: str | None = None,
    **extra: Any,
) -> None:
    """Bind pipeline context for subsequent log events."""

    context: dict[str, Any] = {}
    if pipeline is not None:
        context["pipeline"] = pipeline
    if run_id is not None:
        context["run_id"] = run_id
    if dataset is not None:
        context["dataset"] = dataset
    if component is not None:
        context["component"] = component
    if stage is not None:
        context["stage"] = stage
    if trace_id is not None:
        context["trace_id"] = trace_id
    if span_id is not None:
        context["span_id"] = span_id
    if extra:
        context.update(extra)

    if context:
        UnifiedLogger.bind(**context)


@contextmanager
def pipeline_stage(
    stage: str,
    *,
    pipeline: str,
    run_id: str | None = None,
    dataset: str | None = None,
    component: str | None = None,
    path: Path | str | None = None,
    logger_name: str | None = None,
    **extra: Any,
) -> Iterator[BoundLogger]:
    """Context manager that binds stage-specific logging metadata."""

    resolved_component = component or f"{pipeline}.{stage}"
    with UnifiedLogger.stage(stage, component=resolved_component):
        log = get_pipeline_logger(
            pipeline=pipeline,
            run_id=run_id,
            dataset=dataset,
            stage=stage,
            component=resolved_component,
            path=path,
            logger_name=logger_name,
            **extra,
        )
        yield log
