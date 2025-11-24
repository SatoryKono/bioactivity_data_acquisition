from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from bioetl.core.io import WriteResult
from bioetl.core.pipeline.factory import StageFactory
from bioetl.core.pipeline.orchestration import PipelineBaseCommon
from bioetl.core.pipeline.stages import BaseStageCommand
from bioetl.core.pipeline.types import PipelineExtractionMode, RunResult, StageContext


class _OrchestrationPipeline(PipelineBaseCommon):
    stage_factory_class = StageFactory

    def __init__(self, config: SimpleNamespace, run_id: str, output_dir: Path) -> None:
        self._output_dir_override = output_dir
        super().__init__(config, run_id)
        self.prepare_called = False
        self.finalized_with: RunResult | None = None

    def _resolve_extract_invocation(self) -> tuple[PipelineExtractionMode, list[str] | None]:
        return PipelineExtractionMode.AUTO, None

    def prepare_run(self) -> None:
        self.prepare_called = True

    def extract(self, *, mode: PipelineExtractionMode, ids: list[str] | None) -> pd.DataFrame:
        _ = mode, ids
        return pd.DataFrame({"value": [1, 2]})

    def extract_by_ids(self, ids: list[str]) -> pd.DataFrame:  # pragma: no cover - delegated
        return self.extract(mode=PipelineExtractionMode.AUTO, ids=ids)

    def extract_all(self) -> pd.DataFrame:  # pragma: no cover - delegated
        return self.extract(mode=PipelineExtractionMode.AUTO, ids=None)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(value=df["value"] * 2)

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    def save_results(self, df: pd.DataFrame, output_path: Path, **_: object) -> RunResult:
        write_result = WriteResult(dataset=output_path / "data.csv")
        return RunResult(write_result=write_result, run_directory=output_path)

    def finalize_run(self, result: RunResult | None) -> None:
        self.finalized_with = result

    def _ensure_pipeline_directory(self) -> Path:  # pragma: no cover - deterministic override
        return self._output_dir_override


def _make_config(root: Path) -> SimpleNamespace:
    return SimpleNamespace(
        pipeline=SimpleNamespace(name="orchestration", version="0.0.0"),
        materialization=SimpleNamespace(root=str(root)),
        validation=SimpleNamespace(schema_out=None, schema_in=None, schema_version=None),
        determinism=SimpleNamespace(
            environment=SimpleNamespace(timezone="UTC"),
            hashing=SimpleNamespace(business_key_fields=()),
        ),
        cli=SimpleNamespace(
            date_tag=None,
            extended=False,
            fail_on_schema_drift=True,
            validate_columns=True,
            sample=None,
            limit=None,
            schema=None,
            skip_hash=False,
            fail_on_qc_violation=True,
        ),
        postprocess=SimpleNamespace(correlation=SimpleNamespace(enabled=False)),
    )


def test_pipeline_run_executes_stage_plan(tmp_path: Path) -> None:
    pipeline = _OrchestrationPipeline(_make_config(tmp_path), "run-1", tmp_path)
    result = pipeline.run(tmp_path)

    assert result.dataset_path == tmp_path / "data.csv"
    assert pipeline.prepare_called is True
    assert pipeline.finalized_with is result


class _FailureStage(BaseStageCommand):
    def __init__(self) -> None:
        super().__init__(pipeline=None, name="fail")  # type: ignore[arg-type]

    def execute(self, context: StageContext) -> None:
        raise RuntimeError("boom")


class _FailureFactory(StageFactory):
    def build(self) -> list[BaseStageCommand]:
        return [_FailureStage()]


def test_pipeline_run_propagates_errors(tmp_path: Path) -> None:
    pipeline = _OrchestrationPipeline(_make_config(tmp_path), "run-err", tmp_path)
    pipeline.stage_factory_class = _FailureFactory  # type: ignore[assignment]

    with pytest.raises(RuntimeError):
        pipeline.run(tmp_path)
