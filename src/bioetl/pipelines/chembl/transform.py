from __future__ import annotations

from typing import Any

from bioetl.core.pipeline.unified import PipelineBase
from bioetl.application.pipelines.chembl.stage_runner import run_chembl_stage


def run_transform(pipeline: PipelineBase, df, **kwargs: Any):
    return run_chembl_stage(pipeline, "transform", df=df, **kwargs)

