from __future__ import annotations

from types import SimpleNamespace

from bioetl.core.pipeline.factory import StageFactory


class _StubPipeline(SimpleNamespace):
    def __init__(self) -> None:
        super().__init__(pipeline_code="stub", run_id="run")


def test_stage_factory_builds_default_plan() -> None:
    factory = StageFactory(_StubPipeline())
    plan = factory.build()

    assert [command.name for command in plan] == [
        "extract",
        "transform",
        "validate",
        "write",
        "cleanup",
    ]
