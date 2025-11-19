"""Utilities to register and execute ChEMBL pipeline stages."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from functools import partial
from types import MappingProxyType
from typing import Any, Protocol, runtime_checkable

from structlog.stdlib import BoundLogger, get_logger

from bioetl.config.models.models import PipelineConfig
from bioetl.core.logging import LogEvents, get_pipeline_logger
from bioetl.core.pipeline import PipelineBase

__all__ = [
    "PIPELINE_REGISTRY",
    "PipelineStagesProtocol",
    "StageContext",
    "build_stage_functions",
    "register_pipeline",
    "run_stage",
]

StageCallable = Callable[..., Any]
PipelineLoader = Callable[[], type[PipelineBase]]

_DEFAULT_STAGE_SEQUENCE: tuple[str, ...] = (
    "extract",
    "extract_all",
    "extract_by_ids",
    "transform",
    "validate",
    "write",
)
_KNOWN_STAGE_NAMES: frozenset[str] = frozenset(_DEFAULT_STAGE_SEQUENCE)

_LOG = get_logger(__name__)


@runtime_checkable
class PipelineStagesProtocol(Protocol):
    """Protocol describing the minimal set of stage callables."""

    def extract(self, *args: Any, **kwargs: Any) -> Any:  # pragma: no cover - Protocol hook
        ...

    def extract_all(self, *args: Any, **kwargs: Any) -> Any:  # pragma: no cover - Protocol hook
        ...

    def extract_by_ids(self, *args: Any, **kwargs: Any) -> Any:  # pragma: no cover - Protocol hook
        ...

    def transform(self, *args: Any, **kwargs: Any) -> Any:  # pragma: no cover - Protocol hook
        ...

    def validate(self, *args: Any, **kwargs: Any) -> Any:  # pragma: no cover - Protocol hook
        ...

    def write(self, *args: Any, **kwargs: Any) -> Any:  # pragma: no cover - Protocol hook
        ...


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


@dataclass(slots=True)
class StageContext:
    """Execution context shared between CLI and stage functions."""

    config: PipelineConfig
    run_id: str
    pipeline_name: str | None = None
    stage: str | None = None

    def derive(self, *, stage: str | None = None) -> "StageContext":
        """Return a new context updated with ``stage`` when provided."""

        if stage is None or stage == self.stage:
            return self
        return StageContext(
            config=self.config,
            run_id=self.run_id,
            pipeline_name=self.pipeline_name,
            stage=stage,
        )

    def resolve_pipeline_name(self) -> str:
        """Return the pipeline name, falling back to the config payload."""

        return self.pipeline_name or self.config.pipeline.name


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


def build_stage_functions(
    pipeline_cls: type[PipelineBase] | PipelineLoader,
    *,
    stages: Iterable[str] | None = None,
) -> tuple[PipelineReference, Mapping[str, StageCallable]]:
    """Register ``pipeline_cls`` and return stage callables bound to ``run_stage``."""

    stage_names = _normalize_stage_names(stages)

    if isinstance(pipeline_cls, type):
        _ensure_pipeline_compliance(pipeline_cls, stage_names)
        pipeline_ref = register_pipeline(pipeline_cls)
    elif callable(pipeline_cls):
        def _loader(pipeline_loader: PipelineLoader = pipeline_cls) -> type[PipelineBase]:
            pipeline_type = pipeline_loader()
            _ensure_pipeline_compliance(pipeline_type, stage_names)
            return pipeline_type

        pipeline_ref = register_pipeline(_loader)
    else:  # pragma: no cover - defensive guard
        msg = "build_stage_functions expects a PipelineBase subclass or loader"
        raise TypeError(msg)

    def _make_stage_function(stage_name: str) -> StageCallable:
        """Create a stage function that properly handles StageContext and arguments."""
        partial_func = partial(run_stage, stage_name, pipeline_ref)

        def stage_wrapper(
            context_or_config: StageContext | PipelineConfig,
            run_id_or_arg: str | None = None,
            *args: Any,
            run_id: str | None = None,
            **kwargs: Any,
        ) -> Any:
            """Wrapper that handles StageContext and arguments correctly."""
            # Если передан StageContext, второй позиционный аргумент должен попадать в *args
            if isinstance(context_or_config, StageContext):
                # Если run_id передан как именованный аргумент, это ошибка
                if run_id is not None:
                    msg = "run_stage received redundant run_id alongside StageContext"
                    raise TypeError(msg)
                # Второй позиционный аргумент (run_id_or_arg) должен попадать в *args
                # Собираем все аргументы для stage метода
                stage_args = (run_id_or_arg,) if run_id_or_arg is not None else ()
                stage_args = stage_args + args
                # partial_func уже имеет run_id=None, просто передаем context и аргументы
                return partial_func(context_or_config, *stage_args, **kwargs)
            # Если передан PipelineConfig, второй позиционный аргумент - это run_id
            return partial_func(context_or_config, run_id_or_arg, *args, **kwargs)

        return stage_wrapper

    stage_functions = {
        stage: _make_stage_function(stage) for stage in stage_names
    }
    return pipeline_ref, MappingProxyType(stage_functions)


def _normalize_stage_names(stages: Iterable[str] | None) -> tuple[str, ...]:
    if stages is None:
        return _DEFAULT_STAGE_SEQUENCE

    if isinstance(stages, str):  # pragma: no cover - defensive guard
        msg = "build_stage_functions.stages must be an iterable of stage names"
        raise TypeError(msg)

    normalized = tuple(dict.fromkeys(stage.strip() for stage in stages if stage))
    if not normalized:
        msg = "build_stage_functions.stages must include at least one stage name"
        raise ValueError(msg)

    unknown = [stage for stage in normalized if stage not in _KNOWN_STAGE_NAMES]
    if unknown:
        msg = f"Unsupported stage name(s): {', '.join(sorted(unknown))}"
        raise ValueError(msg)

    return normalized


def _ensure_pipeline_compliance(
    pipeline_cls: type[PipelineBase], stage_names: Sequence[str]
) -> None:
    identifier = _pipeline_identifier(pipeline_cls)
    if not issubclass(pipeline_cls, PipelineStagesProtocol):  # type: ignore[arg-type]
        msg = f"Pipeline '{identifier}' must implement PipelineStagesProtocol"
        raise TypeError(msg)

    available = _available_pipeline_stages(pipeline_cls)
    missing = [stage for stage in stage_names if stage not in available]
    if missing:
        readable_available = ", ".join(available) or "none"
        msg = (
            f"Pipeline '{identifier}' is missing stage(s): {', '.join(missing)}. "
            f"Available stages: {readable_available}"
        )
        raise AttributeError(msg)

    _LOG.debug(
        "pipeline_stage_inventory",
        pipeline_class=_pipeline_identifier(pipeline_cls),
        available_stages=available,
    )


def _available_pipeline_stages(
    pipeline: PipelineBase | type[PipelineBase],
) -> tuple[str, ...]:
    available: list[str] = []
    for stage in _DEFAULT_STAGE_SEQUENCE:
        candidate = getattr(pipeline, stage, None)
        if callable(candidate):
            available.append(stage)
    return tuple(available)


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


def _coerce_stage_context(
    stage: str,
    context_or_config: StageContext | PipelineConfig,
    run_id: str | None,
) -> StageContext:
    if isinstance(context_or_config, StageContext):
        # Если передан StageContext, run_id должен быть None (передан явно как именованный аргумент)
        # Если run_id не None и это строка, это может быть аргумент для stage метода, а не run_id
        # Проверяем только если run_id был передан как именованный аргумент (не позиционный)
        # Для этого нужно проверить, что run_id действительно является run_id, а не аргументом stage
        # Но мы не можем это определить здесь, поэтому просто игнорируем run_id если передан StageContext
        # и считаем, что run_id должен быть явно передан как None или не передан вообще
        # Если run_id не None, это ошибка только если он был передан как именованный аргумент
        # Но мы не можем это определить, поэтому просто игнорируем run_id при StageContext
        return context_or_config.derive(stage=stage)

    if isinstance(context_or_config, PipelineConfig):
        if not run_id:
            msg = "run_stage requires run_id when a PipelineConfig is provided"
            raise TypeError(msg)
        return StageContext(config=context_or_config, run_id=run_id, stage=stage)

    msg = "run_stage expects StageContext or PipelineConfig as the first argument"
    raise TypeError(msg)


def _resolve_stage_callable(pipeline: PipelineBase, stage: str) -> StageCallable:
    try:
        callable_candidate = getattr(pipeline, stage)
    except AttributeError as exc:  # pragma: no cover - defensive guard
        available = _available_pipeline_stages(pipeline)
        readable_available = ", ".join(available) or "none"
        msg = (
            f"Pipeline '{pipeline.__class__.__name__}' does not implement stage '{stage}'. "
            f"Available stages: {readable_available}"
        )
        raise AttributeError(msg) from exc

    if not callable(callable_candidate):  # pragma: no cover - defensive guard
        msg = f"Attribute '{stage}' on '{pipeline.__class__.__name__}' is not callable"
        raise TypeError(msg)
    return callable_candidate


def run_stage(
    stage: str,
    pipeline_ref: PipelineReference | type[PipelineBase],
    context_or_config: StageContext | PipelineConfig,
    run_id: str | None = None,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Instantiate ``pipeline_cls`` and execute ``stage`` with logging."""

    stage_context = _coerce_stage_context(stage, context_or_config, run_id)
    pipeline_cls, identifier = _resolve_pipeline(pipeline_ref)
    pipeline = pipeline_cls(config=stage_context.config, run_id=stage_context.run_id)
    # Если pipeline_name установлен в StageContext, используем его для pipeline_code
    resolved_pipeline_name = stage_context.resolve_pipeline_name()
    if stage_context.pipeline_name is not None and resolved_pipeline_name != pipeline.pipeline_code:
        pipeline.pipeline_code = resolved_pipeline_name
    pipeline_name = resolved_pipeline_name
    stage_name = stage_context.stage or stage
    log = get_pipeline_logger(pipeline=pipeline_name, run_id=stage_context.run_id, stage=stage_name)

    available_stages = _available_pipeline_stages(pipeline)

    stage_alias = _STAGE_EVENT_ALIASES.get(stage, stage)
    context = {
        "pipeline": pipeline_name,
        "stage": stage_name,
        "pipeline_class": identifier,
        "available_stages": available_stages,
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
