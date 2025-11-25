from __future__ import annotations

"""Запись результатов пайплайна ChEMBL."""

from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import yaml


class ChemblWriter:
    """Writer по умолчанию для ChEMBL пайплайнов."""

    def write(
        self, pipeline: "ChemblEntityPipeline", df: pd.DataFrame, output_dir: Path, *, extended: bool = False
    ) -> Path:
        output_dir.mkdir(parents=True, exist_ok=True)
        date_suffix = date.today().isoformat()
        stem = f"{pipeline.entity_name}_chembl"
        dataset_path = output_dir / f"{stem}_all_{date_suffix}.csv"
        quality_report_path = output_dir / f"{stem}_quality_report.csv"
        meta_path = output_dir / f"{stem}_meta.yaml"
        manifest_path = output_dir / f"{stem}_run_manifest.json"

        df.to_csv(dataset_path, index=False)
        self._write_quality_report(df, quality_report_path)

        payload = {
            "run_id": pipeline.run_id,
            "pipeline": pipeline.pipeline_name,
            "entity": pipeline.entity_name,
            "rows": int(df.shape[0]),
            "columns": list(df.columns),
            "generated_at": datetime.utcnow().isoformat(),
        }
        meta_path.write_text(yaml.safe_dump(payload, allow_unicode=True))
        manifest_path.write_text(
            yaml.safe_dump(
                {
                    "run_id": pipeline.run_id,
                    "artifacts": {
                        "dataset": dataset_path.name,
                        "quality_report": quality_report_path.name,
                        "meta": meta_path.name,
                    },
                }
            )
        )

        log_dir = Path("/data/logs") / stem
        log_dir.mkdir(parents=True, exist_ok=True)
        (log_dir / f"{stem}.log").touch()

        if extended:
            pipeline._write_metadata(output_dir, df)
        return dataset_path

    def _write_quality_report(self, df: pd.DataFrame, output_path: Path) -> None:
        summary = {
            "rows": int(df.shape[0]),
            "columns": len(df.columns),
            "missing_values": int(df.isna().sum().sum()) if not df.empty else 0,
        }
        pd.DataFrame([summary]).to_csv(output_path, index=False)


__all__ = ["ChemblWriter"]
