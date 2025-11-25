from __future__ import annotations

import hashlib
import json
import subprocess
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Generic, Mapping, Sequence, TypeVar

import pandas as pd
import pandera as pa
import yaml

from bioetl.core.io.artifacts import RunArtifacts, WriteArtifacts
from bioetl.core.pipeline.types import RunResult


@dataclass(slots=True)
class BatchExtractionStats:
    """Сводные статистики по батчевой выборке."""

    rows: int
    api_calls: int
    cache_hits: int
    success_count: int
    fallback_count: int
    error_count: int
    duration_seconds: float


class PipelineBase(ABC):
    """Интерфейс стадий пайплайна."""

    def __init__(
        self,
        config: Mapping[str, Any],
        *,
        run_id: str | None = None,
        validator: pa.DataFrameSchema | None = None,
    ) -> None:
        self.config = config
        self.run_id = run_id or uuid.uuid4().hex
        self.validator = validator
        self.dry_run = False

    @property
    def pipeline_name(self) -> str:
        return self.__class__.__name__

    @abstractmethod
    def extract(self, *args, **kwargs) -> pd.DataFrame:
        ...

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        ...

    @abstractmethod
    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        ...

    @abstractmethod
    def write(self, df: pd.DataFrame, output_dir: Path, *, extended: bool = False) -> Path:
        ...

    @abstractmethod
    def run(
        self,
        output_dir: Path,
        *,
        extended: bool = False,
        dry_run: bool | None = None,
        limit: int | None = None,
        sample: int | None = None,
    ) -> RunResult:
        ...

    # Hooks ---------------------------------------------------------------
    def prepare_run(self) -> None:  # pragma: no cover - optional hook
        """Вызывается перед началом extract."""

    def finalize_run(self, result: RunResult | None) -> None:  # pragma: no cover
        """Вызывается после завершения write."""


class UnifiedPipelineBase(PipelineBase):
    """Базовая реализация общего жизненного цикла ETL."""

    def __init__(
        self,
        config: Mapping[str, Any],
        *,
        run_id: str | None = None,
        validator: pa.DataFrameSchema | None = None,
    ) -> None:
        super().__init__(config, run_id=run_id, validator=validator)
        self._git_commit = self._resolve_git_commit()
        self._config_hash = self._compute_config_hash(config)

    # Lifecycle -----------------------------------------------------------
    def run(
        self,
        output_dir: Path,
        *,
        extended: bool = False,
        dry_run: bool | None = None,
        limit: int | None = None,
        sample: int | None = None,
    ) -> RunResult:
        started = time.perf_counter()
        if dry_run is not None:
            self.dry_run = dry_run

        self.prepare_run()
        df: pd.DataFrame | None = None
        error: str | None = None
        metrics: dict[str, Any] = {
            "git_commit": self._git_commit,
            "config_hash": self._config_hash,
            "pipeline": self.pipeline_name,
        }
        durations: dict[str, int] = {}

        try:
            if self.dry_run:
                df = self._empty_frame_from_schema()
            else:
                df = self.extract()
            if limit is not None and df is not None:
                df = df.head(limit)
            if sample is not None and df is not None and not df.empty:
                df = df.sample(min(sample, len(df)), random_state=0)
            if df is not None:
                df = self.transform(df)
                df = self._validate_with_schema(df)
                df = self.validate(df)
                df = self._sort_dataframe(df)

            output_dir.mkdir(parents=True, exist_ok=True)
            if df is not None:
                output_path = self.write(df, output_dir, extended=extended)
                metrics["output_path"] = str(output_path)
            if extended:
                self._write_metadata(output_dir, df)
            success = True
        except Exception as exc:  # pragma: no cover - surfaced via RunResult
            error = str(exc)
            success = False
        finally:
            duration = time.perf_counter() - started
            durations["pipeline"] = int(duration * 1000)
            rows = 0 if df is None else int(df.shape[0])
            metrics["duration_seconds"] = duration
            metrics["rows"] = rows

            run_result = RunResult(
                success=success,
                rows=rows,
                artifacts=RunArtifacts(
                    output_dir=output_dir,
                    logs_directory=output_dir / "logs",
                    write_artifacts=WriteArtifacts(
                        data_path=Path(metrics["output_path"]) if "output_path" in metrics else None
                    ),
                ),
                duration_ms=durations,
                error=error,
                metadata={"legacy_metrics": metrics},
            )
            self.finalize_run(run_result)

        return run_result

    # Stage helpers ------------------------------------------------------
    def _validate_with_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.validator is None:
            return df
        return self.validator.validate(df)

    def _sort_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        columns = sorted(df.columns)
        return df.loc[:, columns].sort_values(by=columns).reset_index(drop=True)

    def _empty_frame_from_schema(self) -> pd.DataFrame:
        if self.validator is None:
            return pd.DataFrame()
        columns = {name: pd.Series(dtype=str(schema.dtype)) for name, schema in self.validator.columns.items()}
        return pd.DataFrame(columns)

    # Metadata -----------------------------------------------------------
    def _write_metadata(self, output_dir: Path, df: pd.DataFrame | None) -> None:
        meta_path = output_dir / "meta.yaml"
        manifest_path = output_dir / "run_manifest.json"
        payload = {
            "run_id": self.run_id,
            "pipeline": self.pipeline_name,
            "git_commit": self._git_commit,
            "config_hash": self._config_hash,
            "rows": 0 if df is None else int(df.shape[0]),
            "columns": [] if df is None else list(df.columns),
            "dry_run": self.dry_run,
        }
        meta_path.write_text(yaml.safe_dump(payload, allow_unicode=True))
        manifest = {
            "run_id": self.run_id,
            "artifacts": {
                "meta": meta_path.name,
            },
            "metrics": payload,
        }
        manifest_path.write_text(json.dumps(manifest, indent=2))

    # Utils --------------------------------------------------------------
    def _resolve_git_commit(self) -> str | None:
        try:
            completed = subprocess.run(
                ["git", "rev-parse", "HEAD"], capture_output=True, check=True, text=True
            )
        except Exception:
            return None
        return completed.stdout.strip() or None

    def _compute_config_hash(self, config: Mapping[str, Any]) -> str:
        serialized = yaml.safe_dump(dict(config), sort_keys=True).encode("utf-8")
        return hashlib.sha256(serialized).hexdigest()

    # Default write ------------------------------------------------------
    def write(self, df: pd.DataFrame, output_dir: Path, *, extended: bool = False) -> Path:
        output_dir.mkdir(parents=True, exist_ok=True)
        dataset_path = output_dir / f"{self.pipeline_name}.csv"
        tmp_path = dataset_path.with_suffix(dataset_path.suffix + ".tmp")
        df.to_csv(tmp_path, index=False)
        tmp_path.replace(dataset_path)
        if extended:
            self._write_metadata(output_dir, df)
        return dataset_path


ChemblPipelineT = TypeVar("ChemblPipelineT", bound="ChemblPipelineBase")


class ChemblExtractionDescriptor(Generic[ChemblPipelineT]):
    """Описание извлечения сущности ChEMBL."""

    def __init__(
        self,
        *,
        build_context: Callable[[ChemblPipelineT], Mapping[str, Any]],
        fetcher_factory: Callable[[Mapping[str, Any]], Callable[[Sequence[str] | None], Any]],
        finalizer_factory: Callable[[Mapping[str, Any]], Callable[[pd.DataFrame], pd.DataFrame]],
    ) -> None:
        self.build_context = build_context
        self.fetcher_factory = fetcher_factory
        self.finalizer_factory = finalizer_factory


class CircuitBreakerOpenError(RuntimeError):
    """Исключение, сигнализирующее о срабатывании circuit breaker."""


class ChemblPipelineBase(UnifiedPipelineBase):
    """Базовый пайплайн для ChEMBL с общей логикой выгрузки дескрипторов."""

    def __init__(self, config: Mapping[str, Any], *, run_id: str | None = None) -> None:
        super().__init__(config, run_id=run_id)
        self._chembl_release: str | None = None

    def resolve_chembl_release(self, chembl_client: Any) -> str:
        if self._chembl_release:
            return self._chembl_release
        status = chembl_client.status()
        release = status.get("chembl_release") if isinstance(status, Mapping) else None
        if not release:
            raise RuntimeError("Не удалось определить chembl_release")
        self._chembl_release = str(release)
        return self._chembl_release

    def run_descriptor_extraction(
        self,
        descriptor: ChemblExtractionDescriptor[ChemblPipelineT],
        ids: Sequence[str] | None,
        *,
        summary_event: str,
        metadata_filters: Mapping[str, Any] | None = None,
        fetch_mode: str = "default",
        **batch_kwargs: Any,
    ) -> tuple[pd.DataFrame, BatchExtractionStats]:
        context = dict(descriptor.build_context(self))
        if metadata_filters:
            context["metadata_filters"] = metadata_filters
        context["fetch_mode"] = fetch_mode
        chembl_client = context.get("chembl_client")
        if chembl_client is not None:
            context["chembl_release"] = self.resolve_chembl_release(chembl_client)

        if self.dry_run:
            empty = pd.DataFrame()
            stats = BatchExtractionStats(
                rows=0,
                api_calls=0,
                cache_hits=0,
                success_count=0,
                fallback_count=0,
                error_count=0,
                duration_seconds=0.0,
            )
            return empty, stats

        fetcher = descriptor.fetcher_factory(context)
        finalizer = descriptor.finalizer_factory(context)
        batch_size = min(int(batch_kwargs.get("batch_size", 25)), 25)
        batches = [ids[i : i + batch_size] for i in range(0, len(ids or []), batch_size)]
        if not batches:
            batches = [None]

        start = time.perf_counter()
        frames: list[pd.DataFrame] = []
        api_calls = cache_hits = success = fallback = errors = 0
        for batch in batches:
            try:
                result = fetcher(batch)
                meta: dict[str, Any] = {}
                batch_df: pd.DataFrame
                if isinstance(result, tuple) and len(result) == 2 and isinstance(result[1], Mapping):
                    batch_df = pd.DataFrame(result[0]) if not isinstance(result[0], pd.DataFrame) else result[0]
                    meta = dict(result[1])
                else:
                    batch_df = pd.DataFrame(result)
                frames.append(batch_df)
                api_calls += int(meta.get("api_calls", 0 if meta.get("cache_hit") else 1))
                cache_hits += int(meta.get("cache_hit", False)) * max(len(batch_df), 1)
                fallback += int(meta.get("fallback", 0))
                success += int(batch_df.shape[0])
            except CircuitBreakerOpenError:
                errors += 1
                break
            except Exception:
                errors += 1
                continue

        dataframe = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        dataframe = finalizer(dataframe)
        duration = time.perf_counter() - start
        stats = BatchExtractionStats(
            rows=int(dataframe.shape[0]),
            api_calls=api_calls,
            cache_hits=cache_hits,
            success_count=success,
            fallback_count=fallback,
            error_count=errors,
            duration_seconds=duration,
        )
        return dataframe, stats

