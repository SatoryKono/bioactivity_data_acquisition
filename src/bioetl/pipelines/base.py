"""Base pipeline class."""

from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

import pandas as pd

from bioetl.config import PipelineConfig
from bioetl.core.logger import UnifiedLogger
from bioetl.core.output_writer import (
    AdditionalTableSpec,
    OutputArtifacts,
    OutputMetadata,
    UnifiedOutputWriter,
)
from bioetl.utils.qc import (
    update_summary_metrics,
    update_summary_section,
    update_validation_issue_summary,
)

logger = UnifiedLogger.get(__name__)


class PipelineBase(ABC):
    """Базовый класс для всех пайплайнов."""

    def __init__(self, config: PipelineConfig, run_id: str):
        self.config = config
        self.run_id = run_id
        self.output_writer = UnifiedOutputWriter(run_id)
        self.validation_issues: list[dict[str, Any]] = []
        self.qc_metrics: dict[str, Any] = {}
        self.qc_summary_data: dict[str, Any] = {}
        self.qc_missing_mappings = pd.DataFrame()
        self.qc_enrichment_metrics = pd.DataFrame()
        self.runtime_options: dict[str, Any] = {}
        self.additional_tables: dict[str, AdditionalTableSpec] = {}
        self.export_metadata: OutputMetadata | None = None
        self.debug_dataset_path: Path | None = None
        logger.info("pipeline_initialized", pipeline=config.pipeline.name, run_id=run_id)

    _SEVERITY_LEVELS: dict[str, int] = {"info": 0, "warning": 1, "error": 2, "critical": 3}

    def record_validation_issue(self, issue: dict[str, Any]) -> None:
        """Store a validation issue for later QC reporting."""

        issue = issue.copy()
        issue.setdefault("metric", "validation_issue")
        issue.setdefault("severity", "info")
        self.validation_issues.append(issue)

    def get_runtime_limit(self) -> int | None:
        """Return a positive runtime limit if configured, normalising the value."""

        raw_limit = self.runtime_options.get("limit")
        if raw_limit is None:
            raw_limit = self.runtime_options.get("sample")
        if raw_limit is None:
            return None

        try:
            limit_value = int(raw_limit)
        except (TypeError, ValueError):
            logger.warning(
                "invalid_runtime_limit",
                pipeline=self.config.pipeline.name,
                limit=raw_limit,
            )
            return None

        if limit_value < 1:
            logger.warning(
                "non_positive_runtime_limit",
                pipeline=self.config.pipeline.name,
                limit=limit_value,
            )
            return None

        self.runtime_options["limit"] = limit_value
        return limit_value

    def _severity_value(self, severity: str) -> int:
        """Convert severity label to comparable integer."""

        return self._SEVERITY_LEVELS.get(severity.lower(), 0)

    def _should_fail(self, severity: str) -> bool:
        """Determine if the given severity breaches the configured threshold."""

        threshold = self.config.qc.severity_threshold.lower()
        return self._severity_value(severity) >= self._severity_value(threshold)

    @abstractmethod
    def extract(self, *args: Any, **kwargs: Any) -> pd.DataFrame:
        """Извлекает данные из источника."""
        pass

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Трансформирует данные."""
        pass

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Валидирует данные через Pandera."""
        # Placeholder for now
        return df

    def export(
        self,
        df: pd.DataFrame,
        output_path: Path,
        extended: bool = False,
    ) -> OutputArtifacts:
        """Экспортирует данные с QC отчетами."""
        logger.info("exporting_data", path=output_path, rows=len(df))
        return self.output_writer.write(
            df,
            output_path,
            metadata=self.export_metadata,
            extended=extended,
            issues=self.validation_issues,
            qc_metrics=self.qc_metrics,
            qc_summary=self.qc_summary_data,
            qc_missing_mappings=self.qc_missing_mappings,
            qc_enrichment_metrics=self.qc_enrichment_metrics,
            additional_tables=self.additional_tables,
            runtime_options=self.runtime_options,
            debug_dataset=self.debug_dataset_path,
        )

    def add_additional_table(
        self,
        name: str,
        frame: pd.DataFrame | None,
        *,
        relative_path: Path | str | None = None,
    ) -> None:
        """Register or remove an additional dataset for export."""

        if frame is None or frame.empty:
            self.additional_tables.pop(name, None)
            return

        path_value: Path | None = None
        if relative_path is not None:
            candidate = Path(relative_path)
            if candidate.is_absolute():
                logger.warning(
                    "additional_table_absolute_path_ignored",
                    name=name,
                    path=str(candidate),
                )
            else:
                path_value = candidate

        self.additional_tables[name] = AdditionalTableSpec(
            dataframe=frame,
            relative_path=path_value,
        )

    def remove_additional_table(self, name: str) -> None:
        """Remove a previously registered additional dataset."""

        self.additional_tables.pop(name, None)

    def register_additional_tables(
        self,
        tables: Mapping[str, pd.DataFrame]
        | Iterable[tuple[str, pd.DataFrame, Path | str | None]],
        *,
        clear_existing: bool = True,
    ) -> None:
        """Register multiple additional datasets for export."""

        if clear_existing:
            self.additional_tables.clear()

        if isinstance(tables, Mapping):
            iterator: Iterable[tuple[str, pd.DataFrame, Path | str | None]] = (
                (name, dataframe, None)
                for name, dataframe in tables.items()
            )
        else:
            iterator = tables

        for entry in iterator:
            if len(entry) == 2:
                table_name, frame = entry  # type: ignore[misc]
                relative = None
            else:
                table_name, frame, relative = entry  # type: ignore[misc]
            self.add_additional_table(table_name, frame, relative_path=relative)

    def set_export_metadata(self, metadata: OutputMetadata | None) -> OutputMetadata | None:
        """Persist explicit output metadata for subsequent export."""

        self.export_metadata = metadata
        return metadata

    def build_export_metadata(
        self,
        df: pd.DataFrame,
        *,
        pipeline_version: str,
        source_system: str,
        chembl_release: str | None = None,
        column_order: Sequence[str] | None = None,
    ) -> OutputMetadata:
        """Construct and cache :class:`OutputMetadata` from ``df``."""

        metadata = OutputMetadata.from_dataframe(
            df,
            pipeline_version=pipeline_version,
            source_system=source_system,
            chembl_release=chembl_release,
            column_order=list(column_order) if column_order is not None else list(df.columns),
            run_id=self.run_id,
        )
        self.export_metadata = metadata
        return metadata

    def set_qc_metrics(
        self,
        metrics: Mapping[str, Any],
        *,
        merge: bool = False,
    ) -> None:
        """Store QC metrics and propagate them to the summary payload."""

        if merge and self.qc_metrics:
            self.qc_metrics.update(metrics)
        else:
            self.qc_metrics = dict(metrics)

        if self.qc_metrics:
            update_summary_metrics(self.qc_summary_data, self.qc_metrics)
        else:
            self.qc_summary_data.pop("metrics", None)

    def update_qc_summary_section(
        self,
        section: str,
        values: Mapping[str, Any] | Any,
        *,
        merge: bool = True,
    ) -> Any:
        """Update a QC summary section using unified helpers."""

        return update_summary_section(self.qc_summary_data, section, values, merge=merge)

    def update_qc_summary_metrics(self, metrics: Mapping[str, Any]) -> None:
        """Merge QC metrics into the summary without replacing stored metrics."""

        update_summary_metrics(self.qc_summary_data, metrics)

    def update_qc_validation_summary(self) -> None:
        """Refresh validation issue counters in the QC summary."""

        update_validation_issue_summary(self.qc_summary_data, self.validation_issues)

    def _should_emit_debug_artifacts(self) -> bool:
        """Determine whether verbose/debug outputs should be materialised."""

        cli_options = getattr(self.config, "cli", None)
        verbose = False
        debug = False
        debug_mode = False

        if isinstance(cli_options, dict):
            verbose = bool(cli_options.get("verbose"))
            debug = bool(cli_options.get("debug"))
            mode = cli_options.get("mode")
            if isinstance(mode, str):
                debug_mode = mode.lower() == "debug"

        runtime_verbose = bool(self.runtime_options.get("verbose"))
        runtime_debug = bool(self.runtime_options.get("debug"))

        return verbose or debug or runtime_verbose or runtime_debug or debug_mode

    def _dump_debug_output(self, df: pd.DataFrame, output_path: Path) -> Path | None:
        """Persist the final dataframe as JSON when debug artefacts are enabled."""

        if not self._should_emit_debug_artifacts():
            return None

        dataset_path = output_path
        if dataset_path.suffix != ".csv":
            dataset_path = dataset_path.with_suffix(".csv")

        json_path = dataset_path.with_suffix(".json")

        try:
            self.output_writer.write_dataframe_json(df, json_path)
        except Exception as exc:  # noqa: BLE001 - surface context in logs
            logger.warning(
                "debug_dataset_write_failed",
                path=str(json_path),
                error=str(exc),
            )
            return None

        return json_path

    def run(
        self, output_path: Path, extended: bool = False, *args: Any, **kwargs: Any
    ) -> OutputArtifacts:
        """Запускает полный пайплайн: extract → transform → validate → export."""
        logger.info("pipeline_started", pipeline=self.config.pipeline.name)

        try:
            self.additional_tables = {}
            self.export_metadata = None
            self.debug_dataset_path = None
            # Extract
            df = self.extract(*args, **kwargs)
            logger.info("extraction_completed", rows=len(df))

            # Transform
            df = self.transform(df)
            logger.info("transformation_completed", rows=len(df))

            # Validate
            df = self.validate(df)
            logger.info("validation_completed", rows=len(df))

            self.debug_dataset_path = self._dump_debug_output(df, output_path)

            # Export
            artifacts = self.export(df, output_path, extended=extended)
            logger.info("pipeline_completed", artifacts=str(artifacts.dataset))

            return artifacts

        except Exception as e:
            logger.error("pipeline_failed", error=str(e))
            raise

