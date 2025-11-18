from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from bioetl.core.io import WriteResult
from bioetl.core.pipeline import (
    PipelineBase,
    PipelineExtractionMode,
    PipelineStageCommand,
    RunResult,
    StageContext,
    StageExecutionOptions,
)


class _StageCommandProbePipeline(PipelineBase):
    """Deterministic pipeline that records stage command behaviour."""

    def __init__(self, config, run_id: str) -> None:
        super().__init__(config, run_id)
        self.calls: list[str] = []
        self.save_kwargs: dict[str, bool] = {}

    def prepare_run(self) -> None:  # pragma: no cover - not exercised in these tests
        return

    def extract(
        self,
        *,
        mode: PipelineExtractionMode = PipelineExtractionMode.AUTO,
        ids: tuple[str, ...] | None = None,
    ) -> pd.DataFrame:
        self.calls.append("extract")
        return super().extract(mode=mode, ids=ids)

    def extract_all(self) -> pd.DataFrame:
        return pd.DataFrame({"value": [1, 2, 3]})

    def extract_by_ids(self, ids: tuple[str, ...]) -> pd.DataFrame:  # pragma: no cover - unused helper
        return pd.DataFrame({"value": list(range(len(ids)))})

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.calls.append("transform")
        return df.assign(value=df["value"].astype(int) + 1)

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        self.calls.append("validate")
        return df

    def save_results(
        self,
        df: pd.DataFrame,
        output_path: Path,
        *,
        extended: bool = False,
        include_correlation: bool | None = None,
        include_qc_metrics: bool | None = None,
    ) -> RunResult:
        self.calls.append("write")
        self.save_kwargs = {
            "extended": bool(extended),
            "include_correlation": bool(include_correlation),
            "include_qc_metrics": bool(include_qc_metrics),
        }
        dataset_path = output_path / "dataset.csv"
        output_path.mkdir(parents=True, exist_ok=True)
        dataset_path.write_text("value\n1\n", encoding="utf-8")
        write_result = WriteResult(dataset=dataset_path)
        return RunResult(write_result=write_result, run_directory=output_path, _dataframe=df)

    def finalize_run(self, result: RunResult | None) -> None:  # pragma: no cover - unused hook
        return

    def close_resources(self) -> None:
        self.calls.append("close_resources")
        return


@pytest.fixture(name="stage_pipeline")
def fixture_stage_pipeline(pipeline_config_fixture, run_id: str) -> _StageCommandProbePipeline:
    return _StageCommandProbePipeline(pipeline_config_fixture, run_id)


@pytest.fixture(name="stage_command_plan")
def fixture_stage_command_plan(stage_pipeline: _StageCommandProbePipeline) -> list[PipelineStageCommand]:
    return stage_pipeline.create_stage_factory().build()


@pytest.fixture(name="make_stage_context")
def fixture_make_stage_context(stage_pipeline: _StageCommandProbePipeline, tmp_path: Path):
    def _build_context(**options_kwargs) -> StageContext:
        output_dir = tmp_path / "stage-run"
        output_dir.mkdir(parents=True, exist_ok=True)
        options = StageExecutionOptions(**options_kwargs)
        return StageContext(
            pipeline=stage_pipeline,
            output_dir=output_dir,
            options=options,
            stage_durations_ms={},
        )

    return _build_context


@pytest.fixture(name="stage_context")
def fixture_stage_context(make_stage_context):
    return make_stage_context()


def _get_command(plan: list[PipelineStageCommand], name: str) -> PipelineStageCommand:
    for command in plan:
        if command.name == name:
            return command
    msg = f"Stage '{name}' not found"
    raise AssertionError(msg)


def test_default_stage_plan_executes_in_order(
    stage_command_plan: list[PipelineStageCommand],
    stage_context: StageContext,
    stage_pipeline: _StageCommandProbePipeline,
) -> None:
    for command in stage_command_plan:
        assert command.should_run(stage_context.options)
        command.execute(stage_context)

    assert stage_pipeline.calls == ["extract", "transform", "validate", "write", "close_resources"]
    assert stage_context.result is not None


def test_write_stage_inherits_extended_flags(
    stage_command_plan: list[PipelineStageCommand],
    make_stage_context,
    stage_pipeline: _StageCommandProbePipeline,
) -> None:
    write_command = _get_command(stage_command_plan, "write")
    context = make_stage_context(extended=True, include_qc_metrics=False)
    context.set_payload("validate", pd.DataFrame({"value": [1]}))

    write_command.execute(context)

    assert stage_pipeline.save_kwargs == {
        "extended": True,
        "include_correlation": True,
        "include_qc_metrics": True,
    }


def test_transform_stage_requires_extraction_payload(
    stage_command_plan: list[PipelineStageCommand],
    make_stage_context,
) -> None:
    transform_command = _get_command(stage_command_plan, "transform")
    context = make_stage_context()

    with pytest.raises(KeyError, match="Stage context missing payload 'extract'"):
        transform_command.execute(context)
