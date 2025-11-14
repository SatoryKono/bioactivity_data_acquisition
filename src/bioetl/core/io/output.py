"""Deterministic output helpers used by :class:`PipelineBase`."""

from __future__ import annotations

import csv
import hashlib
import json
import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Literal, TextIO

import pandas as pd
import yaml

from bioetl.core.logging import LogEvents

from .hashing import hash_from_mapping

if TYPE_CHECKING:
    from bioetl.config.models.models import PipelineConfig

# CSV quoting type for pandas to_csv
# Values: 0=QUOTE_MINIMAL, 1=QUOTE_ALL, 2=QUOTE_NONNUMERIC, 3=QUOTE_NONE
CSVQuotingLiteral = Literal[0, 1, 2, 3]

__all__ = [
    "DeterministicWriteArtifacts",
    "WriteArtifacts",
    "RunArtifacts",
    "WriteResult",
    "build_write_artifacts",
    "emit_qc_artifact",
    "ensure_hash_columns",
    "plan_run_artifacts",
    "prepare_dataframe",
    "serialise_metadata",
    "write_dataset_atomic",
    "write_json_atomic",
    "write_frame_like",
    "write_yaml_atomic",
    "build_run_manifest_payload",
]


@dataclass(frozen=True, slots=True)
class DeterministicWriteArtifacts:
    """In-memory view of a dataset prepared for writing."""

    dataframe: pd.DataFrame
    metadata: Mapping[str, Any]


@dataclass(frozen=True)
class WriteArtifacts:
    """Collection of files emitted by the write stage of a pipeline run."""

    dataset: Path
    metadata: Path | None = None
    quality_report: Path | None = None
    correlation_report: Path | None = None
    qc_metrics: Path | None = None


@dataclass(frozen=True)
class RunArtifacts:
    """All artifacts tracked for a completed run."""

    write: WriteArtifacts
    run_directory: Path
    manifest: Path | None
    log_file: Path
    extras: dict[str, Path] = field(default_factory=dict)


@dataclass(frozen=True)
class WriteResult:
    """Materialised artifacts produced by the write stage."""

    dataset: Path
    quality_report: Path | None = None
    metadata: Path | None = None
    correlation_report: Path | None = None
    qc_metrics: Path | None = None
    extras: dict[str, Path] = field(default_factory=dict)


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

    def _needs_recompute(series: pd.Series) -> bool:
        if series.empty:
            return True
        as_string = series.astype("string")
        return bool(as_string.isna().any() or (as_string.str.strip() == "").any())

    if row_column in result.columns:
        row_needs_recompute = _needs_recompute(result[row_column])
    else:
        row_needs_recompute = True

    if row_needs_recompute:
        row_fields = list(row_fields)  # ensure deterministic ordering
        row_records: list[dict[str, Any]] = []
        for tuple_values in result[row_fields].itertuples(index=False, name=None):
            record = dict(zip(row_fields, tuple_values, strict=True))
            row_records.append(record)
        row_hashes = [
            hash_from_mapping(record, row_fields, algorithm=algorithm) for record in row_records
        ]
        result[row_column] = pd.Series(row_hashes, index=result.index, dtype="string")

    if business_fields:
        if business_column in result.columns:
            business_needs_recompute = _needs_recompute(result[business_column])
        else:
            business_needs_recompute = True
        if business_needs_recompute:
            business_records: list[dict[str, Any]] = []
            for tuple_values in result[business_fields].itertuples(index=False, name=None):
                record = dict(zip(business_fields, tuple_values, strict=True))
                business_records.append(record)
            business_hashes = [
                hash_from_mapping(record, business_fields, algorithm=algorithm)
                for record in business_records
            ]
            result[business_column] = pd.Series(
                business_hashes, index=result.index, dtype="string"
            )

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


def _write_mapping_atomic(
    payload: Mapping[str, Any], path: Path, serializer: Callable[[Mapping[str, Any], TextIO], None]
) -> None:
    """Persist ``payload`` using ``serializer`` via an atomic replace."""

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        serializer(payload, handle)
    os.replace(tmp_path, path)


def write_json_atomic(payload: Mapping[str, Any], path: Path) -> None:
    """Write ``payload`` as canonical JSON via an atomic replace."""

    _write_mapping_atomic(
        payload,
        path,
        lambda data, handle: json.dump(
            data,
            handle,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ),
    )


def write_yaml_atomic(payload: Mapping[str, Any], path: Path) -> None:
    """Persist ``payload`` as YAML using an atomic ``os.replace``."""

    _write_mapping_atomic(
        payload,
        path,
        lambda data, handle: yaml.safe_dump(data, handle, sort_keys=True, allow_unicode=True),
    )


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


def _relative_artifact_path(path: Path, base: Path) -> str:
    try:
        return path.relative_to(base).as_posix()
    except ValueError:
        return path.as_posix()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_run_manifest_payload(
    *,
    pipeline_code: str,
    run_id: str,
    run_directory: Path,
    dataset: Path,
    metadata: Path | None,
    quality_report: Path | None,
    correlation_report: Path | None,
    qc_metrics: Path | None,
    extras: Mapping[str, Path],
) -> dict[str, Any]:
    """Return manifest payload capturing deterministic artifact metadata."""

    def _entry(name: str, path: Path) -> dict[str, Any]:
        return {
            "name": name,
            "path": _relative_artifact_path(path, run_directory),
            "size_bytes": path.stat().st_size,
            "sha256": _file_sha256(path),
        }

    entries: list[dict[str, Any]] = []
    if dataset.exists():
        entries.append(_entry("dataset", dataset))
    if metadata and metadata.exists():
        entries.append(_entry("meta", metadata))
    if quality_report and quality_report.exists():
        entries.append(_entry("quality_report", quality_report))
    if correlation_report and correlation_report.exists():
        entries.append(_entry("correlation_report", correlation_report))
    if qc_metrics and qc_metrics.exists():
        entries.append(_entry("qc_metrics", qc_metrics))
    for extra_name, extra_path in sorted(extras.items()):
        if extra_path.exists():
            entries.append(_entry(f"extra:{extra_name}", extra_path))

    entries.sort(key=lambda item: (item["name"], item["path"]))

    return {
        "manifest_version": 1,
        "pipeline": pipeline_code,
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "run_directory": run_directory.as_posix(),
        "artifacts": entries,
        "total_artifacts": len(entries),
    }


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
    log.debug(LogEvents.WRITING_QC_ARTIFACT, artifact=artifact_name, path=str(target_path))
    write_frame_like(frame, target_path, config=config)
    return target_path


def plan_run_artifacts(
    stem: str,
    *,
    run_directory: Path,
    logs_directory: Path,
    dataset_extension: str,
    qc_extension: str,
    manifest_extension: str,
    log_extension: str,
    include_correlation: bool = False,
    include_qc_metrics: bool = False,
    include_metadata: bool = False,
    include_manifest: bool = False,
    extras: Mapping[str, Path] | None = None,
) -> RunArtifacts:
    """Return the artifact map for a deterministic run."""

    dataset = run_directory / f"{stem}.{dataset_extension}"
    quality = run_directory / f"{stem}_quality_report.{qc_extension}"
    correlation = (
        run_directory / f"{stem}_correlation_report.{qc_extension}"
        if include_correlation
        else None
    )
    qc_metrics = run_directory / f"{stem}_qc.{qc_extension}" if include_qc_metrics else None
    metadata = run_directory / f"{stem}_meta.yaml" if include_metadata else None
    manifest = (
        run_directory / f"{stem}_run_manifest.{manifest_extension}" if include_manifest else None
    )
    log_file = logs_directory / f"{stem}.{log_extension}"

    write_artifacts = WriteArtifacts(
        dataset=dataset,
        metadata=metadata,
        quality_report=quality,
        correlation_report=correlation,
        qc_metrics=qc_metrics,
    )
    extras_map: dict[str, Path] = dict(extras) if extras is not None else {}
    return RunArtifacts(
        write=write_artifacts,
        run_directory=run_directory,
        manifest=manifest,
        log_file=log_file,
        extras=extras_map,
    )
