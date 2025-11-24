from __future__ import annotations

from collections.abc import Sequence
from unittest.mock import MagicMock

import pytest
from infrastructure.config.models.models import PipelineConfig
from application.pipelines import PipelineBase
from application.pipelines.specs.chembl.stage_runner import (
    PIPELINE_REGISTRY,
    StageContext,
    build_stage_functions,
    register_pipeline,
)


class _CliStagePipeline(PipelineBase):
    """Minimal pipeline stub used to exercise stage registry helpers."""

    actor = "cli-stage-pipeline"
    id_column = "test_id"

    def _result(
        self,
        stage: str,
        args: tuple[object, ...],
        kwargs: dict[str, object],
    ) -> dict[str, object]:
        return {
            "stage": stage,
            "args": args,
            "kwargs": kwargs,
            "run_id": self.run_id,
            "pipeline": self.config.pipeline.name,
        }

    def extract(self, *args: object, **kwargs: object) -> dict[str, object]:
        return self._result("extract", args, kwargs)

    def extract_all(self) -> dict[str, object]:  # pragma: no cover - protocol hook
        return self._result("extract_all", tuple(), {})

    def extract_by_ids(self, ids: Sequence[str]) -> dict[str, object]:  # pragma: no cover - protocol hook
        return self._result("extract_by_ids", (tuple(ids),), {})

    def transform(self, df: object) -> dict[str, object]:  # pragma: no cover - protocol hook
        return self._result("transform", (df,), {})

    def validate(self, df: object) -> dict[str, object]:  # pragma: no cover - protocol hook
        return self._result("validate", (df,), {})


def test_register_pipeline_populates_registry() -> None:
    reference = register_pipeline(_CliStagePipeline)
    identifier = reference.identifier()

    assert identifier.endswith("._CliStagePipeline")
    assert identifier in PIPELINE_REGISTRY
    assert PIPELINE_REGISTRY[identifier] is _CliStagePipeline


def test_stage_function_executes_with_config(
    pipeline_config_fixture: PipelineConfig,
    run_id: str,
) -> None:
    _, stages = build_stage_functions(_CliStagePipeline, stages=("extract",))

    result = stages["extract"](pipeline_config_fixture, run_id, "payload", source="cli")

    assert result["stage"] == "extract"
    assert result["args"] == ("payload",)
    assert result["kwargs"] == {"source": "cli"}
    assert result["run_id"] == run_id
    assert result["pipeline"] == pipeline_config_fixture.pipeline.name


def test_stage_function_respects_stage_context(
    pipeline_config_fixture: PipelineConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, stages = build_stage_functions(_CliStagePipeline, stages=("extract",))
    context = StageContext(
        config=pipeline_config_fixture,
        run_id="ctx-run",
        pipeline_name="cli-stage",
    )

    captured: dict[str, str] = {}
    logger = MagicMock()

    def _fake_logger(**kwargs):  # type: ignore[no-untyped-def]
        captured.update({
            "pipeline": kwargs.get("pipeline", ""),
            "run_id": kwargs.get("run_id", ""),
            "stage": kwargs.get("stage", ""),
        })
        return logger

    monkeypatch.setattr(
        "application.pipelines.specs.chembl.stage_runner.get_pipeline_logger",
        _fake_logger,
    )

    result = stages["extract"](context)

    assert result["stage"] == "extract"
    assert result["args"] == tuple()
    assert result["run_id"] == "ctx-run"
    assert result["pipeline"] == pipeline_config_fixture.pipeline.name
    assert context.stage is None
    assert captured == {"pipeline": "cli-stage", "run_id": "ctx-run", "stage": "extract"}
