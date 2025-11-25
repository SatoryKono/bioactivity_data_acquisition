from __future__ import annotations

from typing import Any

from bioetl.core.pipeline.unified import PipelineBase
from bioetl.pipelines.chembl.stage_runner import StageRunner


def run_transform(pipeline: PipelineBase, df, **kwargs: Any):  # pragma: no cover
    runner = StageRunner(pipeline)
    return runner.run_stage("transform", df=df, **kwargs)


__all__ = ["run_transform"]

