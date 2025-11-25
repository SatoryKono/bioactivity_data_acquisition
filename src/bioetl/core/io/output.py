from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Literal, Mapping, Sequence

import pandas as pd
import pandera as pa
import yaml

from bioetl.config.models import PipelineConfig
from bioetl.core.logging import UnifiedLogger
from bioetl.core.pipeline.types import WriteResult
from bioetl.core.io.artifacts import (
    DeterminismSettings,
    RunArtifacts,
    SchemaRegistry,
    SchemaRegistryEntry,
    WriteArtifacts,
    compute_file_hash,
    hash_business_key,
    hash_row,
)
from bioetl.qc.plan import QCPlan, QCMetricsExecutor


@dataclass(slots=True)
class AtomicWriter:
    """Атомарная запись файлов через временный путь в той же директории."""

    target_path: Path
    _temp_path: Path = field(init=False)

    def __post_init__(self) -> None:
        self.target_path.parent.mkdir(parents=True, exist_ok=True)
        self._temp_path = self._build_temp_path()

    def _build_temp_path(self) -> Path:
        return self.target_path.with_name(f".{self.target_path.name}.tmp")

    @property
    def temp_path(self) -> Path:
        return self._temp_path

    def write_bytes(self, data: bytes) -> Path:
        with open(self._temp_path, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        self.commit()
        return self.target_path

    def write_text(self, data: str, *, encoding: str = "utf-8") -> Path:
        return self.write_bytes(data.encode(encoding))

    def commit(self) -> Path:
        os.replace(self._temp_path, self.target_path)
        return self.target_path

    def cleanup(self) -> None:
        if self._temp_path.exists():
            try:
                self._temp_path.unlink()
            except FileNotFoundError:
                pass

    def __enter__(self) -> "AtomicWriter":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        if exc is not None:
            self.cleanup()
        elif self._temp_path.exists():
            self.commit()


def _enforce_column_order(df: pd.DataFrame, schema_entry: SchemaRegistryEntry) -> pd.DataFrame:
    if not schema_entry.schema.ordered:
        return df
    if not schema_entry.column_order:
        return df
    missing = [col for col in schema_entry.column_order if col not in df.columns]
    if missing:
        raise pa.errors.SchemaError(
            schema=schema_entry.schema,
            data=df,
            message=f"Dataframe missing ordered columns: {missing}",
        )
    return df.loc[:, list(schema_entry.column_order)]


def _align_for_validation(df: pd.DataFrame, schema_entry: SchemaRegistryEntry) -> pd.DataFrame:
    if schema_entry.schema.ordered and schema_entry.column_order:
        existing = [col for col in schema_entry.column_order if col in df.columns]
        return df.loc[:, existing]
    return df


def _sort_dataframe(
    df: pd.DataFrame, determinism: DeterminismSettings | None
) -> pd.DataFrame:
    if determinism is None or determinism.sort_by is None:
        return df
    missing = [col for col in determinism.sort_by if col not in df.columns]
    if missing:
        raise pa.errors.SchemaError(
            schema=None,  # type: ignore[arg-type]
            data=df,
            message=f"Deterministic sort columns missing: {missing}",
        )
    return df.sort_values(by=list(determinism.sort_by)).reset_index(drop=True)


def _hash_dataframe(df: pd.DataFrame, fields: Sequence[str]) -> str:
    digest_values: list[Any] = []
    subset = df.loc[:, list(fields)] if fields else df
    for _, row in subset.iterrows():
        digest_values.extend(row.tolist())
    return hash_row(digest_values)


def build_meta_yaml(
    pipeline_code: str,
    config_hash: str,
    chembl_release: str | None,
    rows: int,
    artifact_checksums: Mapping[str, str],
    *,
    pipeline_version: str | None = None,
) -> dict[str, Any]:
    return {
        "pipeline_code": pipeline_code,
        "pipeline_version": pipeline_version,
        "config_hash": config_hash,
        "chembl_release": chembl_release,
        "rows": int(rows),
        "artifact_checksums": dict(artifact_checksums),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def write_json_atomic(data: dict[str, Any], path: Path) -> Path:
    payload = json.dumps(data, ensure_ascii=False, indent=2)
    return AtomicWriter(path).write_text(payload)


def write_yaml_atomic(data: dict[str, Any], path: Path) -> Path:
    payload = yaml.safe_dump(data, allow_unicode=True, sort_keys=True)
    return AtomicWriter(path).write_text(payload)


def validate_with_schema(
    df: pd.DataFrame,
    schema_entry: SchemaRegistryEntry,
    *,
    fail_on_schema_drift: bool,
    logger: UnifiedLogger | None = None,
) -> pd.DataFrame:
    try:
        return schema_entry.schema.validate(df)
    except pa.errors.SchemaError:
        if fail_on_schema_drift:
            raise
        if logger:
            logger.warning(
                "SCHEMA_DRIFT_ALLOWED",
                schema=schema_entry.identifier,
                version=schema_entry.version,
            )
        return df


def emit_qc_artifact(
    df: pd.DataFrame,
    artifacts: RunArtifacts,
    *,
    plan: QCPlan | None = None,
    business_key_fields: Sequence[str] | None = None,
) -> Mapping[str, Path]:
    executor = QCMetricsExecutor()
    dataset_name = artifacts.write_artifacts.data_path.stem if artifacts.write_artifacts and artifacts.write_artifacts.data_path else "dataset"
    bundle = executor.execute(
        df,
        plan=plan,
        business_key_fields=business_key_fields,
        dataset_name=dataset_name,
        output_dir=artifacts.output_dir,
    )
    if artifacts.write_artifacts is None:
        artifacts.write_artifacts = WriteArtifacts()
    if bundle.report_paths:
        artifacts.write_artifacts.quality_report_path = bundle.report_paths.get("quality_report")
        artifacts.write_artifacts.qc_summary_path = bundle.report_paths.get("qc_json")
    return bundle.report_paths or {}


def _hash_business_keys(df: pd.DataFrame, fields: Sequence[str]) -> str | None:
    if not fields:
        return None
    subset = df.loc[:, list(fields)]
    digest = hash_business_key(subset.to_numpy().flatten().tolist())
    return digest


class UnifiedOutputWriter:
    def __init__(
        self,
        output_dir: Path,
        pipeline_code: str,
        run_stem: str,
        schema_registry: SchemaRegistry,
        config: PipelineConfig,
        logger: UnifiedLogger,
    ) -> None:
        self.output_dir = output_dir
        self.pipeline_code = pipeline_code
        self.run_stem = run_stem
        self.schema_registry = schema_registry
        self.config = config
        self.logger = logger
        self.config_hash = self._hash_config(config)

    def _hash_config(self, config: PipelineConfig) -> str:
        payload = getattr(config, "model_dump", None)
        if callable(payload):
            data = config.model_dump()
        else:
            data = getattr(config, "__dict__", {})
        serialized = json.dumps(data, sort_keys=True, default=str)
        return hash_row([serialized])

    @property
    def _chembl_release(self) -> str | None:
        metadata = getattr(self.config, "metadata", None) or {}
        return metadata.get("chembl_release") if isinstance(metadata, Mapping) else None

    @property
    def _pipeline_version(self) -> str | None:
        metadata = getattr(self.config, "metadata", None) or {}
        return metadata.get("pipeline_version") if isinstance(metadata, Mapping) else None

    @property
    def _determinism_sort_by(self) -> tuple[str, ...] | None:
        metadata = getattr(self.config, "metadata", None) or {}
        if isinstance(metadata, Mapping):
            determinism = metadata.get("determinism")
            if isinstance(determinism, Mapping):
                sort_cfg = determinism.get("sort")
                if isinstance(sort_cfg, Mapping):
                    sort_by = sort_cfg.get("by")
                    if sort_by:
                        return tuple(sort_by)
        return None

    def write_dataset_atomic(
        self,
        df: pd.DataFrame,
        artifacts: RunArtifacts,
        *,
        format: Literal["csv", "parquet"] = "csv",
        encoding: str = "utf-8",
        index: bool = False,
    ) -> WriteResult:
        schema_entry = self.schema_registry.get(self.pipeline_code)
        fail_on_schema_drift = bool(
            (getattr(self.config, "metadata", None) or {}).get("fail_on_schema_drift", True)
        )
        df_aligned = _align_for_validation(df, schema_entry)
        df_validated = validate_with_schema(
            df_aligned,
            schema_entry,
            fail_on_schema_drift=fail_on_schema_drift,
            logger=self.logger,
        )
        df_ordered = _enforce_column_order(df_validated, schema_entry)
        determinism = schema_entry.determinism or DeterminismSettings(
            sort_by=self._determinism_sort_by
        )
        df_sorted = _sort_dataframe(df_ordered, determinism)

        write_artifacts = artifacts.write_artifacts or WriteArtifacts()
        extension = ".csv" if format == "csv" else ".parquet"
        data_path = write_artifacts.data_path or self.output_dir / f"{self.run_stem}{extension}"
        write_artifacts.data_path = data_path
        write_artifacts.meta_path = write_artifacts.meta_path or self.output_dir / "meta.yaml"
        write_artifacts.manifest_path = write_artifacts.manifest_path or self.output_dir / "run_manifest.json"

        self.output_dir.mkdir(parents=True, exist_ok=True)
        with AtomicWriter(data_path) as writer:
            if format == "csv":
                df_sorted.to_csv(writer.temp_path, index=index, encoding=encoding)
            else:
                df_sorted.to_parquet(writer.temp_path, index=index)
        data_hash = compute_file_hash(data_path)

        artifact_checksums = {data_path.name: data_hash}
        meta_payload = build_meta_yaml(
            pipeline_code=self.pipeline_code,
            pipeline_version=self._pipeline_version,
            config_hash=self.config_hash,
            chembl_release=self._chembl_release,
            rows=int(len(df_sorted)),
            artifact_checksums=artifact_checksums,
        )
        write_yaml_atomic(meta_payload, write_artifacts.meta_path)

        business_key_hash = _hash_business_keys(
            df_sorted, schema_entry.business_key_fields
        )
        row_hash = _hash_dataframe(
            df_sorted, schema_entry.row_hash_fields or tuple(df_sorted.columns)
        )
        manifest_payload = {
            "pipeline_code": self.pipeline_code,
            "run_stem": self.run_stem,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "artifacts": {
                "dataset": {
                    "path": data_path.name,
                    "format": format,
                    "hash": data_hash,
                    "rows": int(len(df_sorted)),
                    "business_key_hash": business_key_hash,
                    "row_hash": row_hash,
                },
                "meta": write_artifacts.meta_path.name,
            },
        }
        write_json_atomic(manifest_payload, write_artifacts.manifest_path)
        artifacts.write_artifacts = write_artifacts

        return WriteResult(rows=int(len(df_sorted)), artifacts=write_artifacts)


__all__ = [
    "AtomicWriter",
    "OutputPlan",
    "OutputWriter",
    "UnifiedOutputWriter",
    "build_meta_yaml",
    "emit_qc_artifact",
    "validate_with_schema",
    "write_json_atomic",
    "write_yaml_atomic",
]
@dataclass(slots=True)
class OutputPlan:
    data_path: Path
    quality_report_path: Path
    meta_path: Path
    manifest_path: Path
    logs_dir: Path
    log_file: Path


class OutputWriter:
    """Планирует и записывает артефакты пайплайна."""

    def __init__(self, base_dir: Path, config: Mapping[str, Any] | PipelineConfig) -> None:
        self.base_dir = base_dir
        self.config = config

    # ------------------------------------------------------------------
    # Публичный интерфейс
    # ------------------------------------------------------------------
    def write_outputs(
        self,
        *,
        df: pd.DataFrame,
        stem: str,
        run_id: str,
        pipeline_name: str,
        entity_name: str,
        quality_report: pd.DataFrame,
    ) -> WriteArtifacts:
        plan = self._plan_paths(stem)
        plan.data_path.parent.mkdir(parents=True, exist_ok=True)

        with AtomicWriter(plan.data_path) as writer:
            df.to_csv(writer.temp_path, index=False)

        plan.quality_report_path.parent.mkdir(parents=True, exist_ok=True)
        with AtomicWriter(plan.quality_report_path) as writer:
            quality_report.to_csv(writer.temp_path, index=False)

        payload = {
            "run_id": run_id,
            "pipeline": pipeline_name,
            "entity": entity_name,
            "rows": int(df.shape[0]),
            "columns": list(df.columns),
            "generated_at": datetime.utcnow().isoformat(),
        }
        write_yaml_atomic(payload, plan.meta_path)

        manifest_payload = {
            "run_id": run_id,
            "artifacts": {
                "dataset": plan.data_path.name,
                "quality_report": plan.quality_report_path.name,
                "meta": plan.meta_path.name,
            },
        }
        write_yaml_atomic(manifest_payload, plan.manifest_path)

        plan.logs_dir.mkdir(parents=True, exist_ok=True)
        plan.log_file.touch()

        artifacts = WriteArtifacts(
            data_path=plan.data_path,
            meta_path=plan.meta_path,
            manifest_path=plan.manifest_path,
            quality_report_path=plan.quality_report_path,
        )
        return artifacts

    # ------------------------------------------------------------------
    # Планирование путей
    # ------------------------------------------------------------------
    def _plan_paths(self, stem: str) -> OutputPlan:
        output_cfg = self._extract_output_cfg()
        base_dir = self._resolve_base_dir(output_cfg)
        date_suffix = date.today().isoformat()

        dataset_default = base_dir / f"{stem}_all_{date_suffix}.csv"
        quality_report_default = base_dir / f"{stem}_quality_report.csv"
        meta_default = base_dir / f"{stem}_meta.yaml"
        manifest_default = base_dir / f"{stem}_run_manifest.json"

        data_path = self._resolve_path(output_cfg.get("dataset_path"), base_dir, dataset_default)
        quality_report_path = self._resolve_path(
            output_cfg.get("quality_report_path"), base_dir, quality_report_default
        )
        meta_path = self._resolve_path(output_cfg.get("meta_path"), base_dir, meta_default)
        manifest_path = self._resolve_path(output_cfg.get("manifest_path"), base_dir, manifest_default)

        logs_dir_cfg = output_cfg.get("logs_dir")
        logs_dir = self._resolve_path(logs_dir_cfg, base_dir, Path("/data/logs") / stem)
        log_file_name = output_cfg.get("log_file") or f"{stem}.log"
        log_file = logs_dir / log_file_name

        return OutputPlan(
            data_path=data_path,
            quality_report_path=quality_report_path,
            meta_path=meta_path,
            manifest_path=manifest_path,
            logs_dir=logs_dir,
            log_file=log_file,
        )

    def _extract_output_cfg(self) -> Mapping[str, Any]:
        if isinstance(self.config, Mapping):
            section = self.config.get("output")
            if isinstance(section, Mapping):
                return section
        metadata = getattr(self.config, "metadata", None)
        if isinstance(metadata, Mapping):
            section = metadata.get("output")
            if isinstance(section, Mapping):
                return section
        return {}

    def _resolve_base_dir(self, output_cfg: Mapping[str, Any]) -> Path:
        root_override = output_cfg.get("root")
        if root_override is None:
            return self.base_dir
        root_path = Path(root_override)
        if not root_path.is_absolute():
            return (self.base_dir / root_path).resolve()
        return root_path

    def _resolve_path(self, candidate: Any, base_dir: Path, default: Path) -> Path:
        if candidate is None:
            return default
        path = Path(candidate)
        if path.is_absolute():
            return path
        return (base_dir / path).resolve()

