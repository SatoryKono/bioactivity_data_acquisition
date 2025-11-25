from __future__ import annotations

from pathlib import Path
from typing import Any

from bioetl.core.pipeline.unified import PipelineBase
from bioetl.pipelines.chembl.stage_runner import StageRunner


def run_write(pipeline: PipelineBase, df, output_dir: Path, **kwargs: Any):
    runner = StageRunner(pipeline)
    return runner.run_stage("write", df=df, output_dir=output_dir, **kwargs)

