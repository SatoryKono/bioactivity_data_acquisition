"""Test runtime base functionality."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

from bioetl.core.pipeline.artifact_runtime_builder import (
    ArtifactRuntimeBuilder
)
from bioetl.core.pipeline.metadata_runtime_builder import (
    MetadataRuntimeBuilder
)
from bioetl.core.pipeline.qc_runtime_builder import QCRuntimeBuilder
from bioetl.core.pipeline.runtime import PipelineRuntimeBase, StagePlanExecutor
from bioetl.core.io.artifacts import RunArtifacts, WriteArtifacts
from bioetl.core.pipeline.types import (
    PipelineStageCommand,
    RunResult,
    StageContext,
    StageExecutionOptions,
    StageRuntimeContext,
)


class RetryableError(Exception):
    pass


class RecordingExecutor(StagePlanExecutor):
    def __init__(self, *, max_retries: int = 0) -> None:
        super().__init__()
        self.max_retries = max_retries
        self.calls: list[tuple[str, int]] = []

    def execute(
        self,
        stages: tuple[PipelineStageCommand, ...],
        context: StageContext,
        options: StageExecutionOptions,
        runtime_context: StageRuntimeContext | None = None,
    ) -> tuple[dict[str, int], str | None]:
        durations: dict[str, int] = {}
        error: str | None = None
        runtime = runtime_context or StageRuntimeContext(
            options=options,
            attributes={},
        )
        if isinstance(runtime.input_data, pd.DataFrame):
            runtime.attributes["last_dataframe"] = (
                runtime.input_data
            )
        for command in stages:
            attempts = 0
            while True:
                attempts += 1
                self.calls.append((command.name, attempts))
                try:
                    runtime.input_data = command.handler(context, runtime)
                    if isinstance(runtime.input_data, pd.DataFrame):
                        runtime.attributes["last_dataframe"] = (
                            runtime.input_data
                        )
                        context.data_bucket.set(runtime.input_data)
                    break
                except RetryableError as exc:
                    if attempts > self.max_retries:
                        error = str(exc)
                        runtime.input_data = None
                        runtime.attributes["last_dataframe"] = None
                        break
                    continue
                except Exception as exc:  # pragma: no cover - defensive
                    error = str(exc)
                    runtime.input_data = None
                    runtime.attributes["last_dataframe"] = None
                    break
            durations[command.name] = durations.get(command.name, 0) + 5
            if error:
                break
        return durations, error


class DummyRuntime(PipelineRuntimeBase):
    def __init__(self, *, executor: RecordingExecutor) -> None:
        super().__init__({}, stage_plan_executor=executor)
        self.executor = executor
        self.calls: list[str] = []
        self.transform_attempts = 0

    def build_stage_plan(
        self, context: StageContext, options: StageExecutionOptions
    ) -> tuple[PipelineStageCommand, ...]:
        """Build stage plan with test commands."""
        self.calls.append("build_stage_plan")

        def _extract(
            ctx: StageContext,
            exec_runtime: StageRuntimeContext,
        ) -> pd.DataFrame:
            self.calls.append("extract")
            meta = exec_runtime.attributes.setdefault("metadata", {})
            meta["extract"] = True
            return pd.DataFrame({"value": [1, 2]})

        def _transform(
            ctx: StageContext,
            exec_runtime: StageRuntimeContext,
        ) -> pd.DataFrame:
            self.calls.append("transform")
            # fail first attempt and succeed on retry
            self.transform_attempts += 1
            if self.transform_attempts == 1:
                raise RetryableError("transient")
            assert exec_runtime.input_data is not None
            return exec_runtime.input_data.assign(
                value=lambda s: s["value"] * 2,
            )

        def _save(
            ctx: StageContext,
            exec_runtime: StageRuntimeContext,
        ) -> WriteArtifacts:
            self.calls.append("save_results")
            artifacts = exec_runtime.attributes.get(
                "artifacts",
            ) or WriteArtifacts()
            artifacts.data_path = (
                exec_runtime.attributes.get("output_dir", Path.cwd())
                / "dataset.csv"
            )
            exec_runtime.attributes["artifacts"] = artifacts
            return artifacts

        return (
            PipelineStageCommand("extract", _extract),
            PipelineStageCommand("transform", _transform),
            PipelineStageCommand("save_results", _save),
        )

    def create_stage_factory(self):  # type: ignore[override]
        """Create stage factory for testing."""
        class _Factory:
            def build(self, descriptors, context, options):
                return descriptors

        return _Factory()


class ServiceRuntime(PipelineRuntimeBase):
    """Test runtime for service functionality."""

    def __init__(self, **kwargs) -> None:
        super().__init__({}, **kwargs)

    def build_stage_plan(
        self, context: StageContext, options: StageExecutionOptions
    ) -> tuple[PipelineStageCommand, ...]:
        def _extract(
            ctx: StageContext,
            exec_runtime: StageRuntimeContext,
        ) -> pd.DataFrame:
            return pd.DataFrame({"value": [1, 2, 3]})

        return (PipelineStageCommand("extract", _extract),)

    def create_stage_factory(self):  # type: ignore[override]
        class _Factory:
            def build(self, descriptors, context, options):
                return descriptors

        return _Factory()


class StubArtifactRuntimeService:
    def __init__(self, target_dir: Path) -> None:
        self.target_dir = target_dir
        self.calls = 0
        self.artifact_planner = MagicMock()
        self.artifact_service = self

    def plan_run_artifacts(
        self,
        output_dir: Path,
        pipeline_code: str,
        run_tag: str | None,
        mode: str | None,
    ) -> tuple[Path, WriteArtifacts]:
        self.calls += 1
        artifacts = WriteArtifacts(
            data_path=self.target_dir / f"{pipeline_code}.csv"
        )
        return self.target_dir, artifacts


class StubQCRuntimeService:
    def __init__(self, qc_path: Path) -> None:
        self.qc_path = qc_path
        self.calls = 0
        self.qc_service = object()
        self.qc_orchestrator = object()

    def run(
        self,
        context: StageContext,
        options: StageExecutionOptions,
    ) -> tuple[Path | None, str | None]:
        self.calls += 1
        return self.qc_path, None


class StubMetadataRuntimeService:
    def __init__(self, logs_dir: Path) -> None:
        self.logs_dir = logs_dir
        self.calls: list[dict[str, object]] = []
        self.metadata_service = MagicMock()
        self.git_commit = "stub_commit"
        self.config_hash = "stub_hash"
        self.logs_directory_resolver = lambda _: self.logs_dir

    def build_run_metadata(
        self,
        context: StageContext,
        stage_plan: tuple[PipelineStageCommand, ...],
        durations: dict[str, int],
        run_tag: str | None,
        mode: str | None,
        *,
        rows: int,
        qc_metrics_path: Path | None,
    ) -> dict[str, object]:
        return {
            "rows": rows,
            "qc_metrics_path": (
                str(qc_metrics_path) if qc_metrics_path else None
            ),
            "durations": durations,
            "run_tag": run_tag,
            "mode": mode,
        }

    def build_run_result(
        self,
        *,
        context: StageContext,
        stage_plan: tuple[PipelineStageCommand, ...],
        run_state,
        run_tag: str | None,
        mode: str | None,
        rows: int,
        qc_metrics_path: Path | None,
        success: bool,
        output_dir: Path,
        logs_directory: Path,
    ) -> RunResult:
        metadata = self.build_run_metadata(
            context,
            stage_plan,
            run_state.durations,
            run_tag,
            mode,
            rows=rows,
            qc_metrics_path=qc_metrics_path,
        )
        self.calls.append(
            {
                "success": success,
                "output_dir": output_dir,
                "logs_directory": logs_directory,
                "qc_metrics_path": qc_metrics_path,
            }
        )
        artifacts = context.artifact_store.get()
        return RunResult(
            success=success,
            rows=rows,
            artifacts=RunArtifacts(
                output_dir=output_dir,
                logs_directory=logs_directory,
                write_artifacts=artifacts,
                qc_metrics_path=qc_metrics_path,
            ),
            duration_ms=run_state.durations,
            error=run_state.error,
            metadata=metadata,
        )


def test_run_handles_retries_and_metadata(tmp_path: Path) -> None:
    executor = RecordingExecutor(max_retries=1)
    runtime = DummyRuntime(executor=executor)

    result = runtime.run(tmp_path)

    assert result.success is True
    assert result.rows == 2
    assert executor.calls == [
        ("extract", 1),
        ("transform", 1),
        ("transform", 2),
        ("save_results", 1),
    ]
    assert result.duration_ms == {
        "extract": 5,
        "transform": 5,
        "save_results": 5,
    }
    assert result.metadata["rows"] == 2
    assert result.artifacts.write_artifacts.data_path == (
        tmp_path / "DummyRuntime.csv"
    )


def test_run_stops_after_retry_exhaustion(tmp_path: Path) -> None:
    executor = RecordingExecutor(max_retries=0)
    runtime = DummyRuntime(executor=executor)

    result = runtime.run(tmp_path)

    assert result.success is False
    assert result.error == "transient"
    # transform fails on first attempt and pipeline stops
    assert executor.calls == [("extract", 1), ("transform", 1)]
    assert result.rows == 2


def test_runtime_uses_injected_services(tmp_path: Path) -> None:
    target_dir = tmp_path / "planned"
    target_dir.mkdir(parents=True, exist_ok=True)
    artifact_runtime_service = StubArtifactRuntimeService(target_dir)
    qc_runtime_service = StubQCRuntimeService(tmp_path / "qc" / "metrics.json")
    metadata_runtime_service = StubMetadataRuntimeService(tmp_path / "logs")

    runtime = ServiceRuntime(
        artifact_runtime_builder=ArtifactRuntimeBuilder(
            runtime_service=artifact_runtime_service
        ),
        qc_runtime_builder=QCRuntimeBuilder(
            qc_runtime_service=qc_runtime_service
        ),
        metadata_runtime_builder=MetadataRuntimeBuilder(
            config={},
            pipeline_code="ServiceRuntime",
            metadata_runtime_service=metadata_runtime_service,
            logs_directory_resolver=lambda output_dir: output_dir / "logs",
        ),
    )

    result = runtime.run(tmp_path, include_qc_metrics=True)

    assert artifact_runtime_service.calls == 1
    assert result.artifacts.write_artifacts.data_path == (
        target_dir / "ServiceRuntime.csv"
    )
    assert qc_runtime_service.calls == 1
    assert result.artifacts.qc_metrics_path == qc_runtime_service.qc_path
    assert metadata_runtime_service.calls[-1][
        "output_dir"
    ] == target_dir
    assert metadata_runtime_service.calls[-1][
        "logs_directory"
    ] == target_dir / "logs"
    assert result.metadata["rows"] == 3


def test_executor_receives_qc_service() -> None:
    executor = RecordingExecutor(max_retries=0)
    runtime = DummyRuntime(executor=executor)

    assert executor.qc_orchestrator is runtime.qc_orchestrator
    assert runtime.qc_orchestrator is not None
    assert runtime.qc_orchestrator.qc_service is runtime.qc_service
