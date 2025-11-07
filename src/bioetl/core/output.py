"""Deterministic output helpers used by :class:`PipelineBase`."""

from __future__ import annotations

import csv
import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import pandas as pd
import yaml

from bioetl.config import PipelineConfig
from bioetl.core.hashing import hash_from_mapping

from .logger import UnifiedLogger

# CSV quoting type for pandas to_csv
# Values: 0=QUOTE_MINIMAL, 1=QUOTE_ALL, 2=QUOTE_NONNUMERIC, 3=QUOTE_NONE
CSVQuotingLiteral = Literal[0, 1, 2, 3]

__all__ = [
    "DeterministicWriteArtifacts",
    "prepare_dataframe",
    "ensure_hash_columns",
    "write_dataset_atomic",
    "write_yaml_atomic",
    "serialise_metadata",
]


@dataclass(frozen=True, slots=True)
class DeterministicWriteArtifacts:
    """In-memory view of a dataset prepared for writing."""

    dataframe: pd.DataFrame
    metadata: Mapping[str, Any]


def ensure_hash_columns(df: pd.DataFrame, *, config: PipelineConfig) -> pd.DataFrame:
    """Return ``df`` with integrity hash columns populated."""

    hashing_config = config.determinism.hashing
    exclude = set(hashing_config.exclude_fields)
    algorithm = hashing_config.algorithm
    row_column = hashing_config.row_hash_column
    business_column = hashing_config.business_key_column

    if hashing_config.row_fields:
        row_fields = list(hashing_config.row_fields)
    else:
        row_fields = [col for col in df.columns if col not in exclude]

    missing_row_fields = [field for field in row_fields if field not in df.columns]
    if missing_row_fields:
        missing_str = ", ".join(missing_row_fields)
        raise KeyError(f"Field(s) {missing_str} is missing from dataframe")

    business_fields = list(hashing_config.business_key_fields)
    missing_business_fields = [field for field in business_fields if field not in df.columns]
    if missing_business_fields:
        missing_str = ", ".join(missing_business_fields)
        raise KeyError(f"Field(s) {missing_str} is missing from dataframe")

    result = df.copy()

    if row_column not in result.columns:
        row_records: list[dict[str, Any]] = []
        for tuple_values in result[row_fields].itertuples(index=False, name=None):
            record = dict(zip(row_fields, tuple_values, strict=True))
            row_records.append(record)
        row_hashes = [
            hash_from_mapping(record, row_fields, algorithm=algorithm) for record in row_records
        ]
        result[row_column] = row_hashes

    if business_fields and business_column not in result.columns:
        business_records: list[dict[str, Any]] = []
        for tuple_values in result[business_fields].itertuples(index=False, name=None):
            record = dict(zip(business_fields, tuple_values, strict=True))
            business_records.append(record)
        business_hashes = [
            hash_from_mapping(record, business_fields, algorithm=algorithm)
            for record in business_records
        ]
        result[business_column] = business_hashes

    return result


def _stable_sort(df: pd.DataFrame, *, config: PipelineConfig) -> pd.DataFrame:
    sort_config = config.determinism.sort
    if not sort_config.by:
        return df

    # Check if all sort columns exist in the DataFrame
    missing_columns = [col for col in sort_config.by if col not in df.columns]

    # If DataFrame is empty and columns are missing, skip sorting (empty DataFrame)
    if df.empty and missing_columns:
        return df

    # If DataFrame is not empty but columns are missing, raise error
    if missing_columns:
        msg = f"Sort columns missing from dataframe: {missing_columns}"
        raise KeyError(msg)

    ascending_list: list[bool] = (
        list(sort_config.ascending) if sort_config.ascending else [True] * len(sort_config.by)
    )
    if sort_config.na_position == "first" or sort_config.na_position == "last":
        na_pos_str = sort_config.na_position
    else:
        na_pos_str = "last"
    assert na_pos_str in ("first", "last")
    na_pos: Literal["first", "last"] = na_pos_str  # type: ignore[assignment]  # assert ensures type
    return df.sort_values(
        by=sort_config.by,
        ascending=ascending_list,
        na_position=na_pos,
        kind="stable",
    ).reset_index(drop=True)


def _enforce_column_order(df: pd.DataFrame, *, config: PipelineConfig) -> pd.DataFrame:
    order = list(config.determinism.column_order)
    if not order:
        return df
    missing = [column for column in order if column not in df.columns]
    if missing:
        msg = f"Column order references missing columns: {missing}"
        raise ValueError(msg)
    extra = [column for column in df.columns if column not in order]
    return df[[*order, *extra]]


def prepare_dataframe(df: pd.DataFrame, *, config: PipelineConfig) -> pd.DataFrame:
    """Apply determinism rules (column order + sort) to ``df``."""

    prepared = _enforce_column_order(df, config=config)
    prepared = _stable_sort(prepared, config=config)
    return prepared


def _csv_quoting(config: PipelineConfig) -> CSVQuotingLiteral:
    quoting_name = config.determinism.serialization.csv.quoting.upper()
    try:
        quote_value = getattr(csv, f"QUOTE_{quoting_name}")
        if not isinstance(quote_value, int):
            msg = f"Invalid CSV quoting constant: {quoting_name}"
            raise ValueError(msg)
        # Ensure it's one of the valid CSV quoting constants
        if quote_value not in (
            csv.QUOTE_ALL,
            csv.QUOTE_MINIMAL,
            csv.QUOTE_NONNUMERIC,
            csv.QUOTE_NONE,
        ):
            msg = f"Invalid CSV quoting constant value: {quote_value}"
            raise ValueError(msg)
        # Return validated quote value (guaranteed to be one of 0,1,2,3)
        return quote_value  # type: ignore[return-value]  # Literal type narrowing
    except AttributeError as exc:  # pragma: no cover - configuration error
        msg = f"Unsupported CSV quoting option: {quoting_name}"
        raise ValueError(msg) from exc


def write_dataset_atomic(df: pd.DataFrame, path: Path, *, config: PipelineConfig) -> None:
    """Write ``df`` deterministically to ``path`` using an atomic replace."""

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    csv_config = config.determinism.serialization.csv
    float_format = f"%.{config.determinism.float_precision}f"
    quoting_value = _csv_quoting(config)
    df.to_csv(
        path_or_buf=str(tmp_path),
        index=False,
        sep=csv_config.separator,
        na_rep=csv_config.na_rep,
        encoding="utf-8",
        quoting=quoting_value,
        lineterminator="\n",
        float_format=float_format,
    )
    os.replace(tmp_path, path)


def write_yaml_atomic(payload: Mapping[str, Any], path: Path) -> None:
    """Persist ``payload`` as YAML using an atomic ``os.replace``."""

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(payload, handle, sort_keys=True, allow_unicode=True)
    os.replace(tmp_path, path)


def write_frame_like(
    frame: pd.DataFrame | Mapping[str, Any], path: Path, *, config: PipelineConfig
) -> None:
    """Write optional QC artefacts respecting deterministic CSV settings."""

    if isinstance(frame, pd.DataFrame):
        dataset = frame
    else:
        dataset = pd.DataFrame([frame])
    write_dataset_atomic(dataset, path, config=config)


def serialise_metadata(
    df: pd.DataFrame,
    *,
    config: PipelineConfig,
    run_id: str,
    pipeline_code: str,
    dataset_path: Path,
    stage_durations_ms: Mapping[str, float],
) -> Mapping[str, Any]:
    """Construct the base metadata payload for ``meta.yaml``."""

    hashing = config.determinism.hashing
    base_metadata: dict[str, Any] = {
        "pipeline": pipeline_code,
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "row_count": int(len(df)),
        "columns": list(df.columns),
        "dataset_path": str(dataset_path),
        "hash_policy_version": config.determinism.hash_policy_version,
        "stage_durations_ms": dict(stage_durations_ms),
        "config_version": config.version,
        "pipeline_version": config.pipeline.version,
        "sorting": {
            "by": list(config.determinism.sort.by),
            "ascending": list(
                config.determinism.sort.ascending or [True] * len(config.determinism.sort.by)
            ),
            "na_position": config.determinism.sort.na_position,
        },
        "hashing": {
            "algorithm": hashing.algorithm,
            "row_fields": list(hashing.row_fields) if hashing.row_fields else [],
            "business_key_fields": list(hashing.business_key_fields),
            "row_column": hashing.row_hash_column,
            "business_key_column": hashing.business_key_column,
        },
    }

    hashing_column_meta: dict[str, Any] = {}
    if hashing.row_hash_column in df.columns:
        column = df[hashing.row_hash_column]
        hashing_column_meta[hashing.row_hash_column] = {
            "unique": int(column.nunique(dropna=False)),
            "nullable": bool(column.isna().any()),
        }
    if hashing.business_key_column in df.columns:
        column = df[hashing.business_key_column]
        hashing_column_meta[hashing.business_key_column] = {
            "unique": int(column.nunique(dropna=False)),
            "nullable": bool(column.isna().any()),
        }
    if hashing_column_meta:
        base_metadata["hashing"].update(hashing_column_meta)

    if config.extends:
        base_metadata["config_extends"] = list(config.extends)
    row_column = hashing.row_hash_column
    business_column = hashing.business_key_column
    if row_column in df.columns and not df.empty:
        base_metadata["hashing"]["sample_hash_row"] = str(df.iloc[0][row_column])
    if business_column in df.columns and not df.empty:
        base_metadata["hashing"]["sample_hash_business_key"] = str(df.iloc[0][business_column])
    return base_metadata


def build_write_artifacts(
    df: pd.DataFrame,
    *,
    config: PipelineConfig,
    run_id: str,
    pipeline_code: str,
    dataset_path: Path,
    stage_durations_ms: Mapping[str, float],
) -> DeterministicWriteArtifacts:
    """Prepare dataframe and metadata for persistent storage."""

    prepared = prepare_dataframe(df, config=config)
    prepared = ensure_hash_columns(prepared, config=config)
    metadata = serialise_metadata(
        prepared,
        config=config,
        run_id=run_id,
        pipeline_code=pipeline_code,
        dataset_path=dataset_path,
        stage_durations_ms=stage_durations_ms,
    )
    return DeterministicWriteArtifacts(dataframe=prepared, metadata=metadata)


def emit_qc_artifact(
    frame: pd.DataFrame | Mapping[str, Any] | None,
    target_path: Path | None,
    *,
    config: PipelineConfig,
    log: Any,
    artifact_name: str,
) -> Path | None:
    """Persist an optional QC artefact and return the resolved path."""

    if frame is None or target_path is None:
        return None
    log.debug("writing_qc_artifact", artifact=artifact_name, path=str(target_path))
    write_frame_like(frame, target_path, config=config)
    return target_path


def finalise_output(
    df: pd.DataFrame,
    *,
    config: PipelineConfig,
    run_id: str,
    pipeline_code: str,
    dataset_path: Path,
    metadata_path: Path,
    stage_durations_ms: Mapping[str, float],
    metadata_hook: Callable[[Mapping[str, Any], pd.DataFrame], Mapping[str, Any]] | None = None,
    quality_report: pd.DataFrame | Mapping[str, Any] | None = None,
    quality_path: Path | None = None,
    correlation_report: pd.DataFrame | Mapping[str, Any] | None = None,
    correlation_path: Path | None = None,
    qc_metrics: pd.DataFrame | Mapping[str, Any] | None = None,
    qc_metrics_path: Path | None = None,
) -> DeterministicWriteArtifacts:
    """Persist the dataset and optional QC artefacts."""

    log = UnifiedLogger.get(__name__)
    log.debug("building_write_artifacts", dataset=str(dataset_path))
    prepared = build_write_artifacts(
        df,
        config=config,
        run_id=run_id,
        pipeline_code=pipeline_code,
        dataset_path=dataset_path,
        stage_durations_ms=stage_durations_ms,
    )
    metadata = dict(prepared.metadata)
    if metadata_hook is not None:
        metadata = dict(metadata_hook(metadata, prepared.dataframe))

    log.debug("writing_dataset", path=str(dataset_path), rows=len(prepared.dataframe))
    write_dataset_atomic(prepared.dataframe, dataset_path, config=config)

    log.debug("writing_metadata", path=str(metadata_path))
    write_yaml_atomic(metadata, metadata_path)

    emit_qc_artifact(
        quality_report, quality_path, config=config, log=log, artifact_name="quality_report"
    )
    emit_qc_artifact(
        correlation_report,
        correlation_path,
        config=config,
        log=log,
        artifact_name="correlation_report",
    )
    emit_qc_artifact(
        qc_metrics, qc_metrics_path, config=config, log=log, artifact_name="qc_metrics"
    )

    return DeterministicWriteArtifacts(dataframe=prepared.dataframe, metadata=metadata)
