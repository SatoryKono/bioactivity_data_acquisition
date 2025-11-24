from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import pandas as pd

from infrastructure.config.models.models import PipelineConfig
from infrastructure.io import WriteResult
from application.pipelines import (
    PipelineBase,
    PipelineExtractionMode,
    PipelineStageCommand,
    RunResult,
    StageContext,
    StageExecutionOptions,
    StageFactory,
)


class _CommandProbePipeline(PipelineBase):
    """Deterministic pipeline used to inspect stage command behaviour."""

    def __init__(self, config: PipelineConfig, run_id: str) -> None:
        super().__init__(config, run_id)
        self.calls: list[str] = []
        self.save_kwargs: dict[str, bool] = {}
        self.fail_on_flags: list[bool] = []

    def prepare_run(self) -> None:
        self.calls.append("prepare")

    def extract(
        self,
        *,
        mode: PipelineExtractionMode = PipelineExtractionMode.AUTO,
        ids: Sequence[str] | None = None,
    ) -> pd.DataFrame:
        self.calls.append("extract")
        return super().extract(mode=mode, ids=ids)

    def extract_all(self) -> pd.DataFrame:
        return pd.DataFrame({"value": [1, 2, 3]})

    def extract_by_ids(self, ids: Sequence[str]) -> pd.DataFrame:
        return pd.DataFrame({"value": list(range(len(ids)))})

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.calls.append("transform")
        return df

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
        self.calls.append("save")
        self.save_kwargs = {
            "extended": bool(extended),
            "include_correlation": bool(include_correlation),
            "include_qc_metrics": bool(include_qc_metrics),
        }
        self.fail_on_flags.append(self._qc_fail_on_threshold)
        dataset_path = output_path / "result.csv"
        dataset_path.parent.mkdir(parents=True, exist_ok=True)
        dataset_path.write_text("value\n1\n", encoding="utf-8")
        write_result = WriteResult(dataset=dataset_path)
        return RunResult(write_result=write_result, run_directory=output_path, _dataframe=df)

    def finalize_run(self, result: RunResult | None) -> None:
        self.calls.append("finalize")


def test_stage_factory_builds_default_plan(pipeline_config_fixture, run_id: str) -> None:
    pipeline = _CommandProbePipeline(pipeline_config_fixture, run_id)
    factory = pipeline.create_stage_factory()

    stage_names = [command.name for command in factory.build()]

    assert stage_names == ["extract", "transform", "validate", "write", "cleanup"]


def test_pipeline_run_uses_custom_stage_plan(
    tmp_path: Path,
    pipeline_config_fixture,
    run_id: str,
) -> None:
    recorder: list[str] = []

    class RecordingCommand(PipelineStageCommand):
        def __init__(self, name: str, *, should_run: bool = True, produce_result: bool = False) -> None:
            self.name = name
            self._should_run = should_run
            self._produce_result = produce_result

        def should_run(self, options: StageExecutionOptions) -> bool:
            recorder.append(f"should:{self.name}")
            return self._should_run

        def execute(self, context: StageContext) -> None:
            recorder.append(f"execute:{self.name}")
            if self._produce_result:
                dataset_path = context.output_dir / f"{self.name}.csv"
                dataset_path.write_text("value\n1\n", encoding="utf-8")
                result = RunResult(
                    write_result=WriteResult(dataset=dataset_path),
                    run_directory=context.output_dir,
                )
                context.set_result(result)

    class StagePlanPipeline(_CommandProbePipeline):
        def create_stage_factory(self) -> StageFactory:
            pipeline = self

            class _Factory(StageFactory):
                def build(self) -> list[PipelineStageCommand]:
                    return [
                        RecordingCommand("extract"),
                        RecordingCommand("transform", should_run=False),
                        RecordingCommand("write", produce_result=True),
                        RecordingCommand("cleanup"),
                    ]

            return _Factory(pipeline)

    pipeline = StagePlanPipeline(pipeline_config_fixture, run_id)
    pipeline.run(tmp_path)

    assert recorder == [
        "should:extract",
        "execute:extract",
        "should:transform",
        "should:write",
        "execute:write",
        "should:cleanup",
        "execute:cleanup",
    ]


def test_write_stage_command_applies_flags(
    tmp_path: Path,
    pipeline_config_fixture,
    run_id: str,
) -> None:
    pipeline = _CommandProbePipeline(pipeline_config_fixture, run_id)
    pipeline.config.cli.extended = True

    pipeline.run(
        tmp_path,
        extended=False,
        include_correlation=False,
        include_qc_metrics=False,
        fail_on_qc_violation=True,
    )

    assert pipeline.save_kwargs == {
        "extended": True,
        "include_correlation": True,
        "include_qc_metrics": True,
    }
    assert pipeline.fail_on_flags == [True]
