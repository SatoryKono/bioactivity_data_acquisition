from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from bioetl.core.pipeline.types import StageDescriptor, StageExecutionOptions


@dataclass(frozen=True, slots=True)
class StagePlanMetadata:
    """Minimal metadata required to tailor the default stage graph."""

    dry_run: bool = False
    has_validator: bool = True


def build_default_stage_plan(
    descriptor: Any | None,
    pipeline_metadata: Mapping[str, Any] | StagePlanMetadata | Any | None = None,
    options: StageExecutionOptions | None = None,
) -> list[StageDescriptor]:
    """Deterministic stage descriptors for the default ETL workflow.

    The plan encodes a linear graph of stages. Runtime concerns such as
    dependency injection are handled elsewhere by :class:`StageFactory`.
    """

    metadata = _normalize_stage_metadata(descriptor, pipeline_metadata, options)
    base_plan: list[StageDescriptor] = _linear_plan()

    if metadata.dry_run:
        base_plan = [stage for stage in base_plan if stage.id != "save_results"]
        if not metadata.has_validator:
            base_plan = [stage for stage in base_plan if stage.id == "extract"]
        base_plan = _relink_plan(base_plan)

    return base_plan

def _normalize_metadata(
    payload: Mapping[str, Any] | StagePlanMetadata | None,
) -> StagePlanMetadata:
    if isinstance(payload, StagePlanMetadata):
        return payload
    if payload is None:
        return StagePlanMetadata()
    return StagePlanMetadata(
        dry_run=bool(payload.get("dry_run", False)),
        has_validator=bool(payload.get("has_validator", True)),
    )


def _normalize_stage_metadata(
    descriptor: Any | None,
    payload: Mapping[str, Any] | StagePlanMetadata | Any | None,
    options: StageExecutionOptions | None,
) -> StagePlanMetadata:
    if isinstance(payload, StageExecutionOptions) and options is None:
        options = payload
        payload = None

    if options is not None:
        has_validator = bool(getattr(descriptor, "validator", None))
        return StagePlanMetadata(dry_run=options.dry_run, has_validator=has_validator)

    return _normalize_metadata(payload)


def _linear_plan() -> list[StageDescriptor]:
    extract = StageDescriptor(id="extract", kind="extract", params={}, next=["transform"])
    transform = StageDescriptor(id="transform", kind="transform", params={}, next=["validate"])
    validate = StageDescriptor(id="validate", kind="validate", params={}, next=["save_results"])
    save_results = StageDescriptor(id="save_results", kind="save_results", params={}, next=[])
    return [extract, transform, validate, save_results]


def _relink_plan(plan: Sequence[StageDescriptor]) -> list[StageDescriptor]:
    relinked: list[StageDescriptor] = []
    for idx, stage in enumerate(plan):
        next_stage = plan[idx + 1].id if idx + 1 < len(plan) else None
        relinked.append(
            StageDescriptor(
                id=stage.id,
                kind=stage.kind,
                params=dict(stage.params),
                next=[next_stage] if next_stage else [],
            )
        )
    return relinked


__all__ = ["StagePlanMetadata", "build_default_stage_plan"]
