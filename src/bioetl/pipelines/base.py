"""Core pipeline orchestration utilities.

This module intentionally focuses on filesystem layout responsibilities so that
pipeline documentation can describe the exact artifact names and retention
rules with executable references.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class WriteArtifacts:
    """Collection of files emitted by the write stage of a pipeline run."""

    dataset: Path
    metadata: Path
    quality_report: Path | None = None
    correlation_report: Path | None = None
    qc_metrics: Path | None = None


@dataclass(frozen=True)
class RunArtifacts:
    """All artifacts tracked for a completed run."""

    write: WriteArtifacts
    run_directory: Path
    manifest: Path
    log_file: Path
    extras: dict[str, Path] = field(default_factory=dict)


class PipelineBase(ABC):
    """Shared orchestration helpers for ETL pipelines."""

    dataset_extension: str = "csv"
    qc_extension: str = "csv"
    manifest_extension: str = "json"
    log_extension: str = "log"
    deterministic_folder_prefix: str = "_"

    def __init__(
        self,
        pipeline_code: str,
        output_root: Path | None = None,
        logs_root: Path | None = None,
        retention_runs: int = 5,
    ) -> None:
        self.pipeline_code = pipeline_code
        self.output_root = Path(output_root or Path("data") / "output")
        self.logs_root = Path(logs_root or Path("data") / "logs")
        self.retention_runs = retention_runs
        self.pipeline_directory = self._ensure_pipeline_directory()
        self.logs_directory = self._ensure_logs_directory()

    def _ensure_pipeline_directory(self) -> Path:
        """Ensure the deterministic output folder exists for the pipeline."""

        directory = self.output_root / f"{self.deterministic_folder_prefix}{self.pipeline_code}"
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    def _ensure_logs_directory(self) -> Path:
        """Ensure the log folder exists for the pipeline."""

        directory = self.logs_root / self.pipeline_code
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    def build_run_stem(self, run_tag: str, mode: str | None = None) -> str:
        """Return the filename stem for artifacts in a run.

        The stem combines the pipeline code, optional mode (e.g. "all" or
        "incremental"), and a deterministic tag such as a run date.
        """
        parts: Sequence[str]
        if mode:
            parts = (self.pipeline_code, mode, run_tag)
        else:
            parts = (self.pipeline_code, run_tag)
        return "_".join(parts)

    def plan_run_artifacts(
        self,
        run_tag: str,
        mode: str | None = None,
        include_correlation: bool = False,
        include_qc_metrics: bool = True,
        extras: dict[str, Path] | None = None,
    ) -> RunArtifacts:
        """Return the artifact map for a deterministic run.

        The directory layout matches the examples in the pipeline catalog where a
        dataset, QC reports, `meta.yaml`, and manifest live side-by-side inside a
        deterministic folder such as ``data/output/_documents``.
        """

        stem = self.build_run_stem(run_tag=run_tag, mode=mode)
        run_dir = self.pipeline_directory
        dataset = run_dir / f"{stem}.{self.dataset_extension}"
        quality = run_dir / f"{stem}_quality_report.{self.qc_extension}"
        correlation = (
            run_dir / f"{stem}_correlation_report.{self.qc_extension}"
            if include_correlation
            else None
        )
        qc_metrics = run_dir / f"{stem}_qc.{self.qc_extension}" if include_qc_metrics else None
        metadata = run_dir / f"{stem}_meta.yaml"
        manifest = run_dir / f"{stem}_run_manifest.{self.manifest_extension}"
        log_file = self.logs_directory / f"{stem}.{self.log_extension}"

        write_artifacts = WriteArtifacts(
            dataset=dataset,
            metadata=metadata,
            quality_report=quality,
            correlation_report=correlation,
            qc_metrics=qc_metrics,
        )
        return RunArtifacts(
            write=write_artifacts,
            run_directory=run_dir,
            manifest=manifest,
            log_file=log_file,
            extras=extras or {},
        )

    def list_run_stems(self) -> Sequence[str]:
        """Return deterministic run stems discovered via ``*_meta.yaml`` files."""

        stems = []
        for meta_file in sorted(
            self.pipeline_directory.glob(f"{self.pipeline_code}_*_meta.yaml"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        ):
            stem = meta_file.stem.rsplit("_meta", 1)[0]
            stems.append(stem)
        return stems

    def apply_retention_policy(self) -> None:
        """Prune older runs beyond the configured ``retention_runs`` count."""

        if self.retention_runs <= 0:
            return

        stems = self.list_run_stems()
        for outdated_stem in stems[self.retention_runs :]:
            for candidate in self._artifact_candidates(outdated_stem):
                if candidate.exists():
                    candidate.unlink()
            log_candidate = self.logs_directory / f"{outdated_stem}.{self.log_extension}"
            if log_candidate.exists():
                log_candidate.unlink()

    def _artifact_candidates(self, stem: str) -> Iterable[Path]:
        """Yield the expected artifact paths for ``stem``."""

        yield self.pipeline_directory / f"{stem}.{self.dataset_extension}"
        yield self.pipeline_directory / f"{stem}_quality_report.{self.qc_extension}"
        yield self.pipeline_directory / f"{stem}_correlation_report.{self.qc_extension}"
        yield self.pipeline_directory / f"{stem}_qc.{self.qc_extension}"
        yield self.pipeline_directory / f"{stem}_meta.yaml"
        yield self.pipeline_directory / f"{stem}_run_manifest.{self.manifest_extension}"

    @abstractmethod
    def extract(self, *args: object, **kwargs: object) -> object:
        """Subclasses fetch raw data and return domain-specific payloads."""

    @abstractmethod
    def transform(self, payload: object) -> object:
        """Subclasses transform raw payloads into normalized tabular data."""

    @abstractmethod
    def run(self, *args: object, **kwargs: object) -> RunArtifacts:
        """Execute the pipeline and return the collected artifacts."""

