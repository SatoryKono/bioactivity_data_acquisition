from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from bioetl.core.pipeline.types import WriteArtifacts, WriteResult


def _sort_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    columns = sorted(df.columns)
    if not columns:
        return df.reset_index(drop=True)
    return df.loc[:, columns].sort_values(by=columns).reset_index(drop=True)


@dataclass(slots=True)
class ArtifactWriter:
    """Детерминированная запись набора данных и метаданных."""

    pipeline_code: str
    run_id: str
    git_commit: str | None
    config_hash: str | None

    def write(
        self,
        df: pd.DataFrame,
        artifacts: WriteArtifacts,
        *,
        output_dir: Path,
        dry_run: bool = False,
        extended: bool = False,
    ) -> WriteResult:
        output_dir.mkdir(parents=True, exist_ok=True)
        dataset_path = (
            artifacts.data_path or output_dir / f"{self.pipeline_code}.csv"
        )
        artifacts.data_path = dataset_path

        sorted_df = _sort_dataframe(df)
        tmp_path = dataset_path.with_suffix(dataset_path.suffix + ".tmp")
        sorted_df.to_csv(tmp_path, index=False)
        tmp_path.replace(dataset_path)

        if extended:
            self.write_metadata(
                output_dir, artifacts, sorted_df, dry_run=dry_run
            )

        return WriteResult(rows=int(sorted_df.shape[0]), artifacts=artifacts)

    def write_metadata(
        self,
        output_dir: Path,
        artifacts: WriteArtifacts,
        df: pd.DataFrame | None,
        *,
        dry_run: bool,
    ) -> None:
        meta_path = artifacts.meta_path or output_dir / "meta.yaml"
        manifest_path = (
            artifacts.manifest_path or output_dir / "run_manifest.json"
        )

        artifacts.meta_path = meta_path
        artifacts.manifest_path = manifest_path

        payload: dict[str, Any] = {
            "run_id": self.run_id,
            "pipeline": self.pipeline_code,
            "git_commit": self.git_commit,
            "config_hash": self.config_hash,
            "rows": 0 if df is None else int(df.shape[0]),
            "columns": [] if df is None else list(df.columns),
            "dry_run": dry_run,
        }
        meta_path.write_text(yaml.safe_dump(payload, allow_unicode=True))

        manifest = {
            "run_id": self.run_id,
            "artifacts": {
                "meta": meta_path.name,
            },
            "metrics": payload,
        }
        manifest_path.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False)
        )


__all__ = ["ArtifactWriter"]
