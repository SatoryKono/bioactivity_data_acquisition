from __future__ import annotations

from typing import Any

from bioetl.core.pipeline.unified import PipelineBase
from bioetl.application.pipelines.chembl.stage_runner import run_chembl_stage


def run_validate(pipeline: PipelineBase, df, **kwargs: Any):  # pragma: no cover
    return run_chembl_stage(pipeline, "validate", df=df, **kwargs)


__all__ = ["run_validate"]

