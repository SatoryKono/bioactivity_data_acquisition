from __future__ import annotations

from typing import Any

from bioetl.core.pipeline.unified import PipelineBase
from bioetl.pipelines.chembl.stage_runner import StageRunner


def run_validate(pipeline: PipelineBase, df, **kwargs: Any):
    runner = StageRunner(pipeline)
    return runner.run_stage("validate", df=df, **kwargs)

