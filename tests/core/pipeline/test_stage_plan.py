from __future__ import annotations

from bioetl.core.pipeline.stage_plan import StagePlanMetadata, build_default_stage_plan


def test_build_default_stage_plan_linear_graph() -> None:
    plan = build_default_stage_plan(None, StagePlanMetadata())

    assert [stage.id for stage in plan] == ["extract", "transform", "validate", "save_results"]
    assert plan[0].next == ["transform"]
    assert plan[-1].next == []


def test_build_default_stage_plan_prunes_save_results_on_dry_run() -> None:
    plan = build_default_stage_plan(None, StagePlanMetadata(dry_run=True, has_validator=True))

    assert [stage.id for stage in plan] == ["extract", "transform", "validate"]
    assert plan[-1].next == []


def test_build_default_stage_plan_prunes_to_extract_without_validator() -> None:
    plan = build_default_stage_plan(None, StagePlanMetadata(dry_run=True, has_validator=False))

    assert [stage.id for stage in plan] == ["extract"]
    assert plan[0].next == []
