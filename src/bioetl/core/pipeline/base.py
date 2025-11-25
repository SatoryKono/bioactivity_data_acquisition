from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import replace
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Iterable, Optional

import pandas as pd
import structlog
import yaml

from .dto import RunResult, StageMetrics, WriteResult
from .utils.interfaces import ErrorAction, ErrorPolicyABC


class PipelineBase(ABC):
    """Базовый интерфейс для управления жизненным циклом ETL-пайплайна."""

    def __init__(
        self,
        run_id: str,
        *,
        error_policy: ErrorPolicyABC | None = None,
        hooks: Iterable["PipelineHookABC"] | None = None,
    ) -> None:
        self.run_id = run_id
        self._error_policy = error_policy
        self._hooks: list[PipelineHookABC] = list(hooks or [])
        self._clients: dict[str, object] = {}
        self.logger = structlog.get_logger().bind(
            pipeline=self.pipeline_name,
            run_id=self.run_id,
        )
        self.dry_run = False

    @property
    def pipeline_name(self) -> str:
        """Человекочитаемое имя пайплайна."""

        return self.__class__.__name__

    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """Извлекает данные из источника."""

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Выполняет преобразования данных."""

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Базовая проверка данных (может быть переопределена)."""

        self.logger.info("validation_skipped", stage="validate", reason="no_schema")
        return df

    def write(self, df: pd.DataFrame, output_path: str | Path) -> WriteResult:
        """Детерминированная атомарная запись DataFrame в CSV/Parquet."""

        path = Path(output_path)
        started_at = datetime.now(timezone.utc)
        self.logger.info("stage_start", stage="write", output=str(path))
        path.parent.mkdir(parents=True, exist_ok=True)

        sort_columns = self.get_sort_columns(df)
        df_to_write = self._sort_dataframe(df, sort_columns)
        format_hint = path.suffix.lower()
        metadata: dict[str, object] | None = None

        with NamedTemporaryFile(delete=False, dir=str(path.parent), suffix=path.suffix) as tmp:
            tmp_path = Path(tmp.name)
        try:
            if format_hint == ".csv":
                df_to_write.to_csv(tmp_path, index=False)
            elif format_hint in {".parquet", ".pq"}:
                df_to_write.to_parquet(tmp_path, index=False)
            else:
                raise ValueError(f"Unsupported output format: {path.suffix}")

            data_hash = self._hash_file(tmp_path)
            tmp_path.replace(path)

            meta_path = path.with_suffix(path.suffix + ".meta.yaml")
            meta_tmp = meta_path.with_suffix(meta_path.suffix + ".tmp")
            metadata = {
                "run_id": self.run_id,
                "pipeline": self.pipeline_name,
                "output_path": str(path),
                "format": "csv" if format_hint == ".csv" else "parquet",
                "rows": int(df_to_write.shape[0]),
                "columns": list(df_to_write.columns),
                "column_hash": self._hash_columns(df_to_write),
                "data_sha256": data_hash,
                "dry_run": False,
                "started_at": started_at.isoformat(),
                "finished_at": datetime.now(timezone.utc).isoformat(),
            }
            meta_tmp.write_text(yaml.safe_dump(metadata, allow_unicode=True))
            meta_tmp.replace(meta_path)
        finally:
            if tmp_path.exists() and not tmp_path.samefile(path):
                tmp_path.unlink(missing_ok=True)
            if "meta_tmp" in locals():
                Path(meta_tmp).unlink(missing_ok=True)

        finished_at = datetime.now(timezone.utc)
        duration = (finished_at - started_at).total_seconds()
        result = WriteResult(
            run_id=self.run_id,
            output_uri=str(path),
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=duration,
            rows_written=df_to_write.shape[0],
            metadata=metadata or {},
        )
        self.logger.info(
            "stage_finished",
            stage="write",
            output=str(path),
            rows_written=df_to_write.shape[0],
            duration_seconds=duration,
        )
        return result

    def run(self, *, output_path: str | Path | None = None, dry_run: bool | None = None) -> RunResult:
        """Запускает пайплайн и возвращает агрегированный результат выполнения."""

        self.dry_run = bool(dry_run) if dry_run is not None else self.dry_run
        started_at = datetime.now(timezone.utc)
        stage_metrics: list[StageMetrics] = []
        self.logger.info("pipeline_start", stage="run", dry_run=self.dry_run)
        for hook in self._hooks:
            hook.before_run(self)

        write_result: WriteResult | None = None
        extracted: pd.DataFrame | None = None
        transformed: pd.DataFrame | None = None
        validated: pd.DataFrame | None = None

        try:
            extracted, metric = self._execute_stage("extract", self.extract)
            stage_metrics.append(metric)
            if extracted is not None:
                transformed, metric = self._execute_stage(
                    "transform", lambda: self.transform(extracted)
                )
                stage_metrics.append(metric)
            else:
                metric = self._skip_stage_metric("transform", "missing_input")
                stage_metrics.append(metric)

            if transformed is not None:
                validated, metric = self._execute_stage(
                    "validate", lambda: self.validate(transformed)
                )
                stage_metrics.append(metric)
            else:
                metric = self._skip_stage_metric("validate", "missing_input")
                stage_metrics.append(metric)

            if validated is not None and not self.dry_run and output_path is not None:
                write_result, metric = self._execute_stage(
                    "write", lambda: self.write(validated, output_path)
                )
                stage_metrics.append(metric)
            else:
                self.logger.info("write_skipped", stage="write", reason="dry_run_or_no_data")
        finally:
            self.close_resources()

        finished_at = datetime.now(timezone.utc)
        run_result = RunResult(
            run_id=self.run_id,
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=(finished_at - started_at).total_seconds(),
            stage_metrics=tuple(stage_metrics),
            total_rows_read=stage_metrics[0].rows_read if stage_metrics else None,
            total_rows_written=write_result.rows_written if write_result else None,
        )
        for hook in self._hooks:
            hook.after_run(self, run_result)

        self.logger.info(
            "pipeline_finished",
            stage="run",
            dry_run=self.dry_run,
            duration_seconds=run_result.duration_seconds,
            rows_read=run_result.total_rows_read,
            rows_written=run_result.total_rows_written,
        )
        return run_result

    def register_hook(self, hook: "PipelineHookABC") -> None:
        """Добавляет хук, который будет вызываться на ключевых этапах выполнения."""

        self._hooks.append(hook)

    def register_client(self, name: str, client: object) -> None:
        """Регистрирует внешние ресурсы для последующего закрытия."""

        self._clients[name] = client

    def close_resources(self) -> None:
        """Закрывает все зарегистрированные клиенты."""

        for name, client in list(self._clients.items()):
            try:
                close_method = getattr(client, "close", None) or getattr(client, "dispose", None)
                if callable(close_method):
                    close_method()
                    self.logger.info("resource_closed", resource=name)
            except Exception as exc:  # pragma: no cover - best effort cleanup
                self.logger.error(
                    "resource_close_failed",
                    resource=name,
                    stage="cleanup",
                    error=str(exc),
                )
            finally:
                self._clients.pop(name, None)

    def _skip_stage_metric(self, stage: str, reason: str) -> StageMetrics:
        """Формирует метрики для пропущенной стадии."""

        now = datetime.now(timezone.utc)
        self.logger.info("stage_skipped", stage=stage, reason=reason)
        return StageMetrics(
            stage_name=stage,
            started_at=now,
            finished_at=now,
            duration_seconds=0.0,
        )

    def get_sort_columns(self, df: pd.DataFrame) -> list[str]:
        """Возвращает список колонок для сортировки."""

        return sorted(df.columns.tolist())

    def _execute_stage(self, name: str, func):
        stage = _FunctionStage(name, func)
        for hook in self._hooks:
            hook.before_stage(stage)
        started_at = datetime.now(timezone.utc)
        self.logger.info("stage_start", stage=name)
        attempts = 0
        result = None
        while True:
            attempts += 1
            try:
                result = func()
                break
            except Exception as exc:  # pragma: no cover - policy controlled
                action = self._error_policy.decide(exc) if self._error_policy else ErrorAction.FAIL
                self.logger.error(
                    "stage_failed",
                    stage=name,
                    attempt=attempts,
                    action=action.value,
                    error=str(exc),
                )
                if action == ErrorAction.RETRY:
                    continue
                if action == ErrorAction.SKIP:
                    result = None
                    break
                raise
        finished_at = datetime.now(timezone.utc)
        duration = (finished_at - started_at).total_seconds()
        metrics = StageMetrics(
            stage_name=name,
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=duration,
        )
        if isinstance(result, pd.DataFrame):
            metrics = replace(metrics, rows_read=result.shape[0])
        elif isinstance(result, WriteResult):
            metrics = replace(metrics, rows_written=result.rows_written)
        for hook in self._hooks:
            hook.after_stage(stage)
        self.logger.info("stage_finished", stage=name, duration_seconds=duration)
        return result, metrics

    def _sort_dataframe(self, df: pd.DataFrame, columns: Iterable[str] | None = None) -> pd.DataFrame:
        columns = list(columns or df.columns)
        sorted_df = df.loc[:, columns]
        return sorted_df.sort_values(by=columns).reset_index(drop=True)

    def _hash_file(self, path: Path) -> str:
        digest = sha256()
        with path.open("rb") as fp:
            for chunk in iter(lambda: fp.read(8192), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _hash_columns(self, df: pd.DataFrame) -> str:
        digest = sha256()
        digest.update("|".join(df.columns).encode())
        return digest.hexdigest()


class PipelineHookABC(ABC):
    """Хуки позволяют расширять поведение пайплайна пользовательскими действиями."""

    @abstractmethod
    def before_run(self, pipeline: PipelineBase) -> None:
        """Вызывается перед запуском пайплайна."""

    @abstractmethod
    def after_run(self, pipeline: PipelineBase, result: RunResult) -> None:
        """Вызывается после завершения пайплайна."""

    @abstractmethod
    def before_stage(self, stage: "StageABC") -> None:
        """Вызывается перед выполнением отдельной стадии."""

    @abstractmethod
    def after_stage(self, stage: "StageABC") -> None:
        """Вызывается после выполнения отдельной стадии."""


class StageABC(ABC):
    """Стадия пайплайна (extract/transform/load и т.д.)."""

    name: str

    @abstractmethod
    def run(self) -> None:
        """Выполняет работу стадии."""

    @abstractmethod
    def dependencies(self) -> Iterable["StageABC"]:
        """Возвращает список стадий, которые должны быть выполнены раньше."""


class _FunctionStage(StageABC):
    def __init__(self, name: str, func) -> None:
        self.name = name
        self._func = func

    def run(self) -> None:  # pragma: no cover - adapter class
        self._func()

    def dependencies(self) -> Iterable["StageABC"]:
        return ()


class CLICommandABC(ABC):
    """Базовый интерфейс для команд CLI, управляющих пайплайном."""

    @abstractmethod
    def name(self) -> str:
        """Имя команды (используется в CLI-фреймворке)."""

    @abstractmethod
    def run(self, argv: Optional[list[str]] = None) -> int:
        """Выполнение команды, возвращает код выхода."""
