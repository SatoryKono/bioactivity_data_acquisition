from __future__ import annotations

from pathlib import Path
from typing import Any

from bioetl.core.pipeline.unified import PipelineBase
from bioetl.pipelines.chembl.stage_runner import StageRunner


def run_extract(pipeline: PipelineBase, **kwargs: Any):
    runner = StageRunner(pipeline)
    return runner.run_stage("extract", **kwargs)

