from __future__ import annotations

from pathlib import Path
from typing import Any

from bioetl.core.pipeline.unified import PipelineBase
from bioetl.pipelines.chembl.stage_runner import run_chembl_stage


def run_extract(pipeline: PipelineBase, **kwargs: Any):
    return run_chembl_stage(pipeline, "extract", **kwargs)

