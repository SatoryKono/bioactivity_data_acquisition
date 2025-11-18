"""Utilities to register and execute ChEMBL pipeline stages."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Protocol

from structlog.stdlib import BoundLogger

from bioetl.config.models.models import PipelineConfig
from bioetl.core.logging import LogEvents, get_pipeline_logger
from bioetl.core.pipeline import PipelineBase

__all__ = ["PIPELINE_REGISTRY", "register_pipeline", "run_stage"]

StageCallable = Callable[..., Any]
PipelineLoader = Callable[[], type[PipelineBase]]


class _PipelineResolver(Protocol):
    """Protocol describing objects returning pipeline classes on demand."""

    def resolve(self) -> type[PipelineBase]:  # pragma: no cover - structural protocol
        """Return the concrete pipeline class."""

    def identifier(self) -> str:
        """Return the stable identifier for the pipeline class."""


@dataclass(slots=True)
class PipelineReference:
    """Container that lazily resolves pipeline classes for stage execution."""

    loader: PipelineLoader
    _identifier: str | None = None
    _pipeline_cls: type[PipelineBase] | None = None

    def resolve(self) -> type[PipelineBase]:
        if self._pipeline_cls is None:
            pipeline_cls = self.loader()
            if not isinstance(pipeline_cls, type) or not issubclass(pipeline_cls, PipelineBase):
                msg = "Pipeline loader must return a PipelineBase subclass"
                raise TypeError(msg)
            self._pipeline_cls = pipeline_cls
        return self._pipeline_cls

    def identifier(self) -> str:
        if self._identifier is None:
            self._identifier = _pipeline_identifier(self.resolve())
        return self._identifier


def _pipeline_identifier(pipeline_cls: type[PipelineBase]) -> str:
    return f"{pipeline_cls.__module__}.{pipeline_cls.__qualname__}"


def _resolve_pipeline(pipeline: PipelineReference | type[PipelineBase]) -> tuple[type[PipelineBase], str]:
    if isinstance(pipeline, PipelineReference):
        pipeline_cls = pipeline.resolve()
        return pipeline_cls, pipeline.identifier()
    if isinstance(pipeline, type) and issubclass(pipeline, PipelineBase):
        identifier = _pipeline_identifier(pipeline)
        return pipeline, identifier
    msg = "run_stage received an invalid pipeline reference"
    raise TypeError(msg)


_PIPELINE_REGISTRY: dict[str, type[PipelineBase]] = {}
PIPELINE_REGISTRY = MappingProxyType(_PIPELINE_REGISTRY)


def register_pipeline(
    pipeline: type[PipelineBase] | PipelineLoader,
) -> PipelineReference:
    """Register a pipeline class or loader and return a reference."""

    if isinstance(pipeline, type):
        identifier = _pipeline_identifier(pipeline)
        existing = _PIPELINE_REGISTRY.get(identifier)
        if existing is not None and existing is not pipeline:
            msg = f"Pipeline '{identifier}' is already registered"
            raise ValueError(msg)
        _PIPELINE_REGISTRY[identifier] = pipeline
        return PipelineReference(loader=lambda pipeline_cls=pipeline: pipeline_cls, _identifier=identifier)

    if callable(pipeline):
        return PipelineReference(loader=pipeline)

    msg = "register_pipeline expects a PipelineBase subclass or loader"
    raise TypeError(msg)


_STAGE_EVENT_ALIASES: dict[str, str] = {
    "extract": "extract",
    "extract_all": "extract",
    "extract_by_ids": "extract",
    "transform": "transform",
    "validate": "validate",
    "write": "write",
}

_STAGE_EVENTS: dict[str, tuple[LogEvents, LogEvents]] = {
    "extract": (LogEvents.STAGE_EXTRACT_START, LogEvents.STAGE_EXTRACT_FINISH),
    "transform": (LogEvents.STAGE_TRANSFORM_START, LogEvents.STAGE_TRANSFORM_FINISH),
    "validate": (LogEvents.STAGE_VALIDATE_START, LogEvents.STAGE_VALIDATE_FINISH),
    "write": (LogEvents.STAGE_WRITE_START, LogEvents.STAGE_WRITE_FINISH),
}


def _log_stage_event(log: BoundLogger, stage_alias: str, start: bool, **context: Any) -> None:
    events = _STAGE_EVENTS.get(stage_alias)
    if not events:
        return
    event = events[0] if start else events[1]
    log.info(event, **context)


def _resolve_stage_callable(pipeline: PipelineBase, stage: str) -> StageCallable:
    try:
        callable_candidate = getattr(pipeline, stage)
    except AttributeError as exc:  # pragma: no cover - defensive guard
        msg = f"Pipeline '{pipeline.__class__.__name__}' does not implement stage '{stage}'"
        raise AttributeError(msg) from exc

    if not callable(callable_candidate):  # pragma: no cover - defensive guard
        msg = f"Attribute '{stage}' on '{pipeline.__class__.__name__}' is not callable"
        raise AttributeError(msg)
    return callable_candidate


def run_stage(
    stage: str,
    pipeline_ref: PipelineReference | type[PipelineBase],
    config: PipelineConfig,
    run_id: str,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Instantiate ``pipeline_cls`` and execute ``stage`` with logging."""

    pipeline_cls, identifier = _resolve_pipeline(pipeline_ref)
    pipeline = pipeline_cls(config=config, run_id=run_id)
    pipeline_name = config.pipeline.name
    log = get_pipeline_logger(pipeline=pipeline_name, run_id=run_id, stage=stage)

    stage_alias = _STAGE_EVENT_ALIASES.get(stage, stage)
    context = {
        "pipeline": pipeline_name,
        "stage": stage,
        "pipeline_class": identifier,
    }

    log.info(LogEvents.STAGE_RUN_START, **context)
    _log_stage_event(log, stage_alias, True, **context)

    stage_callable = _resolve_stage_callable(pipeline, stage)
    try:
        result = stage_callable(*args, **kwargs)
    except Exception:
        log.exception(LogEvents.STAGE_RUN_ERROR, **context)
        raise

    log.info(LogEvents.STAGE_RUN_FINISH, **context)
    _log_stage_event(log, stage_alias, False, **context)
    return result
