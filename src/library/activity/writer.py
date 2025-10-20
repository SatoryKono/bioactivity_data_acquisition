"""Output writer for activity ETL artefacts (assay-aligned)."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import pandas as pd

from structlog.stdlib import BoundLogger

from library.etl.load import write_deterministic_csv
from .config import ActivityConfig
from .pipeline import ActivityETLResult


def _calculate_checksum(file_path: Path) -> str:
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256_hash.update(chunk)
    return sha256_hash.hexdigest()


def write_activity_outputs(
    *,
    result: ActivityETLResult,
    output_dir: Path,
    date_tag: str,
    config: ActivityConfig,
    logger: BoundLogger | None = None,
) -> dict[str, Path]:
    """Persist ETL artefacts and return generated file paths."""

    output_dir.mkdir(parents=True, exist_ok=True)

    data_path = output_dir / (f"activity_{date_tag}.csv" if date_tag else "activity.csv")
    qc_path = output_dir / (f"activity_{date_tag}_qc.csv" if date_tag else "activity_qc.csv")
    meta_path = output_dir / (f"activity_{date_tag}_meta.yaml" if date_tag else "activity_meta.yaml")

    # Save main data deterministically
    # Защита от несоответствия длины ascending и ключей сортировки: безопасный детерминизм
    from library.config import DeterminismSettings as _DeterminismSettings
    safe_det = _DeterminismSettings()
    # Полный порядок колонок: сначала заданные в конфиге (если существуют), затем все прочие, чтобы ничего не терять
    df_cols = list(result.activities.columns)
    base_order = [col for col in config.determinism.column_order if col in df_cols]
    # Гарантируем наличие служебных полей в выводе
    for extra in ["extracted_at", "hash_business_key", "hash_row"]:
        if extra in df_cols and extra not in base_order:
            base_order.append(extra)
    extras = [col for col in df_cols if col not in base_order]
    safe_det.column_order = base_order + extras
    # Ключи сортировки только для существующих колонок
    safe_det.sort.by = [col for col in config.determinism.sort.by if col in df_cols]
    # Приводим ascending к корректному виду
    asc = config.determinism.sort.ascending
    if isinstance(asc, list):
        if len(asc) == len(config.determinism.sort.by):
            # Отфильтруем по фактическим колонкам
            keep_indexes = [i for i, col in enumerate(config.determinism.sort.by) if col in result.activities.columns]
            safe_det.sort.ascending = [asc[i] for i in keep_indexes]
        else:
            safe_det.sort.ascending = True
    else:
        safe_det.sort.ascending = bool(asc)

    # Если всё же длина ascending не совпадает, полностью отключаем сортировку
    try:
        if isinstance(safe_det.sort.ascending, list) and len(safe_det.sort.ascending) != len(safe_det.sort.by):
            safe_det.sort.by = []
            safe_det.sort.ascending = True
    except Exception:
        safe_det.sort.by = []
        safe_det.sort.ascending = True

    write_deterministic_csv(
        result.activities,
        data_path,
        logger=logger,
        determinism=safe_det,
        output=None,
    )

    # Save QC
    if isinstance(result.qc, pd.DataFrame) and not result.qc.empty:
        result.qc.to_csv(qc_path, index=False)

    # Save metadata (YAML)
    try:
        import yaml
    except Exception:  # pragma: no cover - fallback if yaml unavailable
        yaml = None

    meta: dict[str, Any] = dict(result.meta)
    meta.setdefault("row_count", int(len(result.activities)))
    meta.setdefault("pipeline_version", "1.0.0")

    meta["file_checksums"] = {"csv": _calculate_checksum(data_path)}

    if yaml is not None:
        with meta_path.open("w", encoding="utf-8") as handle:
            yaml.dump(meta, handle, default_flow_style=False, allow_unicode=True)
    else:  # pragma: no cover - rare environment
        # Minimal JSON fallback
        import json

        with meta_path.with_suffix(".json").open("w", encoding="utf-8") as handle:
            import numpy as np

            def default(o: Any):
                if isinstance(o, (pd.Series, pd.DataFrame)):
                    return o.to_dict()
                if isinstance(o, (np.integer, )):
                    return int(o)
                if isinstance(o, (np.floating, )):
                    return float(o)
                return str(o)

            json.dump(meta, handle, indent=2, ensure_ascii=False, default=default)

    paths: dict[str, Path] = {"csv": data_path, "meta": meta_path}
    if isinstance(result.qc, pd.DataFrame) and not result.qc.empty:
        paths["qc"] = qc_path

    return paths


__all__ = ["write_activity_outputs"]


