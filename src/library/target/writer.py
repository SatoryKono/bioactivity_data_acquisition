"""Target data writer."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from library.common.writer_base import ETLResult
from library.target.config import TargetConfig


def write_target_outputs(
    result: ETLResult,
    output_dir: Path | str,
    date_tag: str,
    config: TargetConfig,
) -> dict[str, Path]:
    """Write target pipeline outputs to files."""
    
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    outputs = {}
    
    # Write main targets data
    if not result.data.empty:
        targets_file = output_dir / f"targets_{date_tag}.csv"
        result.data.to_csv(targets_file, index=False)
        outputs["targets"] = targets_file
    
    # Write metadata
    if result.metadata:
        metadata_file = output_dir / f"targets_metadata_{date_tag}.json"
        import json
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(result.metadata.model_dump(), f, indent=2, default=str)
        outputs["metadata"] = metadata_file
    
    # Write QC results if available
    if result.qc_summary is not None and not result.qc_summary.empty:
        qc_file = output_dir / f"targets_qc_{date_tag}.csv"
        result.qc_summary.to_csv(qc_file, index=False)
        outputs["qc"] = qc_file
    
    return outputs
