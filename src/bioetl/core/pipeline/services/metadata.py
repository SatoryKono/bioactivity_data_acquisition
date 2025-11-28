"""Metadata services for pipeline execution."""

from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, cast

from bioetl.core.io.artifacts import RunArtifacts, WriteArtifacts
from bioetl.core.pipeline.types import (
    PipelineBaseProtocol,
    RunResult,
    RunState,
    StageContextProtocol,
    StageProtocol,
)
from bioetl.core.runtime.metadata import MetadataCoordinator


class RunMetadataBuilder:
    """Конструктор метаданных запуска пайплайна."""

    __slots__ = ("pipeline_code", "_git_commit", "_config_hash")

    def __init__(
        self, config: Mapping[str, Any] | Any, pipeline_code: str
    ) -> None:
        self.pipeline_code = pipeline_code
        self._git_commit = self._resolve_git_commit()
        self._config_hash = self._compute_config_hash(config)

    @property
    def git_commit(self) -> str | None:
        """Return the git commit hash."""
        return self._git_commit

    @property
    def config_hash(self) -> str | None:
        """Return the configuration hash."""
        return self._config_hash

    def build(
        self,
        context: StageContextProtocol,
        stages: Iterable[StageProtocol],
        durations: Mapping[str, int],
        run_tag: str | None,
        mode: str | None,
    ) -> dict[str, Any]:
        """Build the metadata dictionary."""
        metadata: dict[str, Any] = {
            "stage_plan": [stage.name for stage in stages],
            "extract_metadata": context.metadata,
            "git_commit": self._git_commit,
            "config_hash": self._config_hash,
            "pipeline": self.pipeline_code,
            "run_tag": run_tag,
            "mode": mode,
            "duration_seconds": sum(durations.values()) / 1000,
        }
        artifacts = context.artifact_store.get()
        any_artifacts = cast(Any, artifacts)
        if any_artifacts.data_path:
            metadata["output_path"] = str(any_artifacts.data_path)
        pipeline_metadata = self._collect_pipeline_metadata(context)
        if pipeline_metadata:
            metadata = self._merge_metadata(metadata, pipeline_metadata)
        return metadata

    def _collect_pipeline_metadata(
        self, context: StageContextProtocol
    ) -> Mapping[str, Any]:  # pragma: no cover - thin adapter
        pipeline = getattr(context, "pipeline", None)
        if pipeline is None:
            return {}
        builder = getattr(pipeline, "build_pipeline_metadata", None)
        if callable(builder):
            try:
                extra = builder(context)
            except TypeError:
                extra = builder()
            if isinstance(extra, Mapping):
                return extra
            try:
                return dict(extra)
            # pylint: disable=broad-exception-caught
            except Exception:  # noqa: BLE001
                return {}
        return {}

    @staticmethod
    def _merge_metadata(
        base: dict[str, Any], extra: Mapping[str, Any]
    ) -> dict[str, Any]:  # pragma: no cover - pure function
        merged = dict(base)
        for key, value in extra.items():
            if (
                key == "extract_metadata"
                and isinstance(value, Mapping)
                and isinstance(base.get(key), Mapping)
            ):
                combined = dict(cast(Mapping[str, Any], base[key]))
                combined.update(value)
                merged[key] = combined
            else:
                merged[key] = value
        return merged

    def _resolve_git_commit(self) -> str | None:
        try:
            completed = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                capture_output=True,
                check=True,
                text=True,
            )
        except (subprocess.SubprocessError, OSError):
            return None
        return completed.stdout.strip() or None

    def _compute_config_hash(
        self, config: Mapping[str, Any] | Any
    ) -> str | None:
        try:
            payload: Mapping[str, Any]
            if isinstance(config, Mapping):
                payload = dict(config)
            elif hasattr(config, "__dict__"):
                payload = dict(config.__dict__)
            else:
                return None
            serialized = json.dumps(
                payload,
                sort_keys=True,
                default=str,
            ).encode("utf-8")
            return hashlib.sha256(serialized).hexdigest()
        except (TypeError, ValueError):
            return None


@dataclass(slots=True)
class MetadataService:
    """Service delegating metadata building to injected builder."""

    builder: Any
    _git_commit: str | None = None
    _config_hash: str | None = None

    def __post_init__(self) -> None:  # pragma: no cover - trivial
        self._git_commit = getattr(self.builder, "git_commit", None)
        self._config_hash = getattr(self.builder, "config_hash", None)

    def build(
        self,
        context: StageContextProtocol,
        stage_plan: Iterable[StageProtocol],
        durations: Mapping[str, int],
        run_tag: str | None,
        mode: str | None,
    ) -> dict[str, Any]:
        """Build metadata dictionary using the internal builder."""
        return cast(
            dict[str, Any],
            self.builder.build(
                context, stage_plan, durations, run_tag, mode
            ),
        )

    def build_for_run(
        self,
        context: StageContextProtocol,
        stage_plan: Iterable[StageProtocol],
        durations: Mapping[str, int],
        run_tag: str | None,
        mode: str | None,
        *,
        rows: int,
        qc_metrics_path: Path | None,
    ) -> dict[str, Any]:
        """Build metadata enriched with run stats (rows, QC path)."""
        metadata = self.build(context, stage_plan, durations, run_tag, mode)
        metadata["rows"] = rows
        if qc_metrics_path is not None:
            metadata["qc_metrics_path"] = str(qc_metrics_path)
        return metadata

    @property
    def git_commit(self) -> str | None:
        """Return the git commit hash."""
        return self._git_commit

    @property
    def config_hash(self) -> str | None:
        """Return the configuration hash."""
        return self._config_hash


@dataclass(slots=True)
class MetadataRuntimeService:
    """Runtime coordinator for building run metadata and results."""

    metadata_service: MetadataService
    logs_directory_resolver: Callable[[Path], Path]
    builder: Any | None = None
    git_commit: str | None = None
    config_hash: str | None = None

    def __post_init__(self) -> None:  # pragma: no cover - trivial
        self.builder = getattr(self.metadata_service, "builder", None)
        self.git_commit = getattr(self.metadata_service, "git_commit", None)
        self.config_hash = getattr(self.metadata_service, "config_hash", None)

    def build_run_metadata(
        self,
        context: StageContextProtocol,
        stage_plan: Iterable[StageProtocol],
        durations: Mapping[str, int],
        run_tag: str | None,
        mode: str | None,
        *,
        rows: int,
        qc_metrics_path: Path | None,
    ) -> dict[str, Any]:
        """
        Build comprehensive run metadata.

        Collects info from metadata service and adds runtime stats.
        """
        if hasattr(self.metadata_service, "build_for_run"):
            return cast(
                dict[str, Any],
                self.metadata_service.build_for_run(
                    context,
                    stage_plan,
                    durations,
                    run_tag,
                    mode,
                    rows=rows,
                    qc_metrics_path=qc_metrics_path,
                ),
            )
        builder = getattr(self.metadata_service, "builder", None)
        if builder is not None and callable(builder):
            return cast(
                dict[str, Any],
                builder(
                    context,
                    stage_plan,
                    durations,
                    run_tag,
                    mode,
                    rows=rows,
                    qc_metrics_path=qc_metrics_path,
                ),
            )
        if hasattr(self.metadata_service, "build"):
            return cast(
                dict[str, Any],
                self.metadata_service.build(
                    context,
                    stage_plan,
                    durations,
                    run_tag,
                    mode,
                ),
            )
        return {}

    def build_run_result(
        self,
        *,
        context: StageContextProtocol,
        stage_plan: Iterable[StageProtocol],
        run_state: RunState,
        run_tag: str | None,
        mode: str | None,
        rows: int,
        qc_metrics_path: Path | None,
        success: bool,
        output_dir: Path,
        logs_directory: Path,
    ) -> RunResult:
        """
        Construct the final RunResult object.

        Aggregates metadata, artifacts, stats, and error info.
        """
        resolved_logs_directory = (
            logs_directory
            or self.logs_directory_resolver(output_dir)
        )
        metadata = self.build_run_metadata(
            context,
            stage_plan,
            run_state.durations,
            run_tag,
            run_state.mode if hasattr(run_state, "mode") else mode,
            rows=rows,
            qc_metrics_path=qc_metrics_path,
        )
        if context.artifact_store:
            artifacts = context.artifact_store.get()
        elif run_state.artifacts is not None:
            artifacts = run_state.artifacts
        else:
            write_artifacts_cls = cast(Any, WriteArtifacts)
            artifacts = cast(WriteArtifacts, write_artifacts_cls())
        run_artifacts_cls = cast(Any, RunArtifacts)
        run_artifacts = cast(
            RunArtifacts,
            run_artifacts_cls(
                output_dir=output_dir,
                logs_directory=resolved_logs_directory,
                write_artifacts=artifacts,
                qc_metrics_path=qc_metrics_path,
            ),
        )
        return RunResult(
            success=success,
            rows=rows,
            artifacts=run_artifacts,
            duration_ms=run_state.durations,
            error=run_state.error,
            metadata=metadata,
        )


def default_metadata_service_factory(
    config: Mapping[str, Any] | Any | None = None,
    pipeline_code: str | None = None,
) -> Callable[[PipelineBaseProtocol], MetadataService]:
    """Create a factory for the default metadata service."""
    def _factory(pipeline: PipelineBaseProtocol) -> MetadataService:
        resolved_config = (
            config if config is not None else getattr(pipeline, "config", {})
        )
        raw_code = pipeline_code or pipeline.pipeline_code
        resolved_code = str(raw_code)
        builder = RunMetadataBuilder(resolved_config, resolved_code)
        return MetadataService(builder=builder)

    return _factory


def default_metadata_runtime_service_factory(
    *,
    config: Mapping[str, Any] | Any | None = None,
    pipeline_code: str | None = None,
    metadata_service: MetadataService | None = None,
    metadata_service_factory: Callable[
        [MetadataCoordinator], MetadataService
    ]
    | None = None,
    run_metadata_builder: RunMetadataBuilder | None = None,
    logs_directory_resolver: Callable[[Path], Path] | None = None,
) -> Callable[[MetadataCoordinator], MetadataRuntimeService]:
    """Create a factory for the default metadata runtime service."""
    def _factory(coordinator: MetadataCoordinator) -> MetadataRuntimeService:
        if metadata_service is not None:
            resolved_service = metadata_service
        elif metadata_service_factory is not None:
            resolved_service = metadata_service_factory(coordinator)
        else:
            resolved_config = (
                config
                if config is not None
                else getattr(coordinator, "config", {})
            )
            resolved_code = str(
                pipeline_code or getattr(coordinator, "pipeline_code", "")
            )
            builder = run_metadata_builder or RunMetadataBuilder(
                resolved_config, resolved_code
            )
            resolved_service = MetadataService(builder=builder)
        resolver = logs_directory_resolver or getattr(
            coordinator, "logs_directory_resolver"
        )
        return MetadataRuntimeService(
            metadata_service=resolved_service,
            logs_directory_resolver=resolver,
        )

    return _factory
