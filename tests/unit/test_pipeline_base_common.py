from __future__ import annotations

from types import SimpleNamespace
from pathlib import Path

import pandas as pd

from application.pipelines.common import BaseStageCommand, PipelineBaseCommon, StageContext


class _DummyConfig(SimpleNamespace):
    def __init__(self, root: Path) -> None:
        super().__init__(
            pipeline=SimpleNamespace(name="dummy", version="0.0.0"),
            materialization=SimpleNamespace(root=str(root)),
            validation=SimpleNamespace(schema_out=None, schema_in=None, schema_version=None),
            determinism=SimpleNamespace(hashing=SimpleNamespace(business_key_fields=())),
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
        )


class _ReadStage(BaseStageCommand):
    def __init__(self, pipeline: PipelineBaseCommon) -> None:
        super().__init__(pipeline, "read")

    def execute(self, context: StageContext) -> None:
        context.set_payload("data", self.pipeline.read_input_table("table"))


class _ValidateStage(BaseStageCommand):
    def __init__(self, pipeline: PipelineBaseCommon) -> None:
        super().__init__(pipeline, "validate")

    def execute(self, context: StageContext) -> None:
        validated = self.pipeline.validate(context.require_payload("data"))
        context.set_payload("data", validated)


class _WriteStage(BaseStageCommand):
    def __init__(self, pipeline: PipelineBaseCommon) -> None:
        super().__init__(pipeline, "write")

    def execute(self, context: StageContext) -> None:
        data = context.require_payload("data")
        self.pipeline.write("output", data)
        context.set_result({"data": data, "run_id": self.pipeline.run_id})


class _SimpleStageFactory:
    def __init__(self, pipeline: PipelineBaseCommon) -> None:
        self.pipeline = pipeline

    def build(self):
        return [_ReadStage(self.pipeline), _ValidateStage(self.pipeline), _WriteStage(self.pipeline)]


class _MockPipeline(PipelineBaseCommon):
    def __init__(self, config: _DummyConfig, run_id: str = "run-1") -> None:
        self.pre_called = False
        self.post_called = False
        super().__init__(config, run_id)

    def _pre_run_hook(self, stage_context: StageContext) -> None:
        self.pre_called = True
        super()._pre_run_hook(stage_context)

    def _post_run_hook(self, result, stage_context: StageContext, **kwargs):  # type: ignore[override]
        self.post_called = True
        return super()._post_run_hook(result, stage_context, **kwargs)

    def create_stage_factory(self):  # type: ignore[override]
        return _SimpleStageFactory(self)

    def read_input_table(self, table_ref: str):  # type: ignore[override]
        return pd.DataFrame({"value": [1, 2, 3], "table": [table_ref] * 3})

    def write(self, output_ref: str, data):  # type: ignore[override]
        self.written = (output_ref, data)
        return data

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    def extract(self, *, mode=None, ids=None):
        return pd.DataFrame()

    def extract_all(self):  # type: ignore[override]
        return pd.DataFrame()

    def extract_by_ids(self, ids):  # type: ignore[override]
        return pd.DataFrame()


def test_run_invokes_hooks_and_returns_result(tmp_path):
    config = _DummyConfig(tmp_path)
    pipeline = _MockPipeline(config)

    result = pipeline.run(tmp_path)

    assert pipeline.pre_called is True
    assert pipeline.post_called is True
    assert result == {"data": pipeline.written[1], "run_id": pipeline.run_id}
