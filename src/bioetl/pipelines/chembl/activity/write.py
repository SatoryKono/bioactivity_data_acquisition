from __future__ import annotations

from pathlib import Path
from typing import Any

from bioetl.core.pipeline.unified import PipelineBase
from bioetl.application.pipelines.chembl.stage_runner import run_chembl_stage


def run_write(pipeline: PipelineBase, df, output_dir: Path, **kwargs: Any):  # pragma: no cover - proxy
    return run_chembl_stage(
        pipeline,
        "save_results",
        df=df,
        output_dir=output_dir,
        **kwargs,
    )


__all__ = ["run_write"]

