"""Helpers for deterministic materialization of pipeline artefacts."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, MutableMapping

import pandas as pd

from bioetl.config.models import DeterminismConfig, MaterializationPaths
from bioetl.core.logger import UnifiedLogger
from bioetl.core.output_writer import (
    OutputMetadata,
    UnifiedOutputWriter,
    _resolve_date_format,
    _resolve_float_format,
    extension_for_format,
    normalise_output_format,
)

logger = UnifiedLogger.get(__name__)

DEFAULT_SILVER_PATH = Path("data/output/target/targets_silver.parquet")
DEFAULT_GOLD_PATH = Path("data/output/target/targets_final.parquet")


@dataclass
class MaterializationManager:
    """Coordinate writing of pipeline artefacts with deterministic behaviour."""

    paths: MaterializationPaths
    runtime: Any | None = None
    stage_context: MutableMapping[str, Any] | None = None
    determinism: DeterminismConfig | None = None
    output_writer_factory: Callable[[], UnifiedOutputWriter] | None = None
    _materialization_writer: UnifiedOutputWriter | None = field(
        default=None,
        init=False,
        repr=False,
    )

    def materialize_silver(
        self,
        uniprot_df: pd.DataFrame,
        component_df: pd.DataFrame,
        *,
        format: str = "parquet",
        output_path: Path | str | None = None,
    ) -> dict[str, Path]:
        """Persist UniProt enrichment artefacts for the silver layer."""

        if not self._should_materialize("silver"):
            return {}

        if uniprot_df.empty and component_df.empty:
            logger.info("silver_materialization_skipped", reason="empty_frames")
            self._update_stage_context("silver", {"status": "skipped", "reason": "empty_frames"})
            return {}

        resolved_format = normalise_output_format(format)

        silver_override = Path(output_path) if output_path is not None else None
        silver_path = (
            silver_override
            or self.paths.resolve_dataset_path("silver", "uniprot", resolved_format)
            or self._resolve_default_stage_path(
                DEFAULT_SILVER_PATH,
                resolved_format,
                fallback_stem="targets_silver",
            )
        )
        silver_path.parent.mkdir(parents=True, exist_ok=True)

        outputs: dict[str, Path] = {}

        if not uniprot_df.empty:
            sorted_uniprot = uniprot_df.sort_values("canonical_accession", kind="stable")
            logger.info(
                "writing_silver_dataset",
                path=str(silver_path),
                rows=len(sorted_uniprot),
            )
            self._write_dataframe(sorted_uniprot, silver_path, resolved_format)
            outputs["uniprot"] = silver_path

        component_path = (
            self.paths.resolve_dataset_path("silver", "component_enrichment", resolved_format)
            or self._resolve_sibling_path(
                silver_path.parent,
                "component_enrichment",
                resolved_format,
            )
        )
        if not component_df.empty:
            sorted_components = component_df.sort_values(
                ["canonical_accession", "isoform_accession"],
                kind="stable",
            )
            logger.info(
                "writing_component_enrichment",
                path=str(component_path),
                rows=len(sorted_components),
            )
            self._write_dataframe(sorted_components, component_path, resolved_format)
            outputs["component_enrichment"] = component_path
        elif component_path.exists():
            logger.info("component_enrichment_empty", path=str(component_path))

        if outputs:
            self._update_stage_context("silver", {"outputs": outputs})

        return outputs

    def _get_output_writer(self) -> UnifiedOutputWriter | None:
        """Return (and cache) the configured output writer instance if available."""

        if self.output_writer_factory is None:
            return None

        if self._materialization_writer is None:
            self._materialization_writer = self.output_writer_factory()

        return self._materialization_writer

    def materialize_iuphar(
        self,
        classification_df: pd.DataFrame,
        gold_df: pd.DataFrame,
        *,
        format: str = "parquet",
        output_directory: Path | str | None = None,
    ) -> dict[str, Path]:
        """Persist IUPHAR enrichment artefacts."""

        if not self._should_materialize("iuphar"):
            return {}

        if classification_df.empty and gold_df.empty:
            logger.info("iuphar_materialization_skipped", reason="empty_frames")
            self._update_stage_context("iuphar", {"status": "skipped", "reason": "empty_frames"})
            return {}

        resolved_format = normalise_output_format(format)
        configured_dir: Path | None = None
        if output_directory is not None:
            configured_dir = Path(output_directory)
            if configured_dir.suffix:
                configured_dir = configured_dir.parent

        classification_path = self.paths.resolve_dataset_path("iuphar", "classification", resolved_format)
        gold_path = self.paths.resolve_dataset_path("iuphar", "iuphar_gold", resolved_format)

        output_dir = configured_dir
        if output_dir is None:
            if classification_path is not None:
                output_dir = classification_path.parent
            elif gold_path is not None:
                output_dir = gold_path.parent

        if output_dir is None:
            gold_base = self._resolve_default_stage_path(
                DEFAULT_GOLD_PATH,
                resolved_format,
                fallback_stem=DEFAULT_GOLD_PATH.stem,
            )
            output_dir = gold_base.parent

        output_dir.mkdir(parents=True, exist_ok=True)

        classification_path = classification_path or self._resolve_sibling_path(
            output_dir,
            "targets_iuphar_classification",
            resolved_format,
        )
        gold_path = gold_path or self._resolve_sibling_path(
            output_dir,
            "targets_iuphar_enrichment",
            resolved_format,
        )

        outputs: dict[str, Path] = {}

        if not classification_df.empty:
            sorted_classification = classification_df.sort_values(
                ["target_chembl_id", "iuphar_family_id"],
                kind="stable",
            )
            outputs.update(
                self._write_stage_dataset(
                    sorted_classification,
                    classification_path,
                    resolved_format,
                    dataset="classification",
                )
            )

        if not gold_df.empty:
            sorted_gold = gold_df.sort_values(["target_chembl_id"], kind="stable")
            outputs.update(
                self._write_stage_dataset(
                    sorted_gold,
                    gold_path,
                    resolved_format,
                    dataset="iuphar_gold",
                )
            )

        if outputs:
            self._update_stage_context("iuphar", {"outputs": outputs})

        return outputs

    def materialize_gold(
        self,
        targets: pd.DataFrame,
        components: pd.DataFrame,
        protein_class: pd.DataFrame,
        xref: pd.DataFrame,
        *,
        format: str = "parquet",
        output_path: Path | str | None = None,
    ) -> dict[str, Path]:
        """Persist gold-layer artefacts respecting runtime dry-run settings."""

        if not self._should_materialize("gold"):
            return {}

        resolved_format = normalise_output_format(format)

        targets_override = Path(output_path) if output_path is not None else None
        targets_path = (
            targets_override
            or self.paths.resolve_dataset_path("gold", "targets", resolved_format)
            or self._resolve_default_stage_path(
                DEFAULT_GOLD_PATH,
                resolved_format,
                fallback_stem=DEFAULT_GOLD_PATH.stem,
            )
        )
        base_dir = targets_path.parent
        base_dir.mkdir(parents=True, exist_ok=True)

        outputs: dict[str, Path] = {}

        outputs.update(
            self._write_stage_dataset(
                targets,
                targets_path,
                resolved_format,
                dataset="targets",
            )
        )

        outputs.update(
            self._write_stage_dataset(
                components,
                self._resolve_dataset_output_path(
                    base_dir,
                    "gold",
                    "target_components",
                    resolved_format,
                    fallback_stem="target_components",
                ),
                resolved_format,
                dataset="target_components",
            )
        )
        outputs.update(
            self._write_stage_dataset(
                protein_class,
                self._resolve_dataset_output_path(
                    base_dir,
                    "gold",
                    "protein_class",
                    resolved_format,
                    fallback_stem="protein_class",
                ),
                resolved_format,
                dataset="protein_class",
            )
        )
        outputs.update(
            self._write_stage_dataset(
                xref,
                self._resolve_dataset_output_path(
                    base_dir,
                    "gold",
                    "target_xref",
                    resolved_format,
                    fallback_stem="target_xref",
                ),
                resolved_format,
                dataset="target_xref",
            )
        )

        if outputs:
            self._update_stage_context("gold", {"outputs": outputs})

        return outputs

    # Internal helpers -------------------------------------------------

    def _write_stage_dataset(
        self,
        df: pd.DataFrame,
        path: Path,
        format: str,
        *,
        dataset: str,
    ) -> dict[str, Path]:
        if df.empty:
            logger.info("gold_materialization_skipped", dataset=dataset, reason="empty_dataframe")
            return {}

        path.parent.mkdir(parents=True, exist_ok=True)
        logger.info("writing_gold_dataset", dataset=dataset, path=str(path), rows=len(df))
        self._write_dataframe(df, path, format)
        return {dataset: path}

    def _write_dataframe(self, df: pd.DataFrame, path: Path, format: str) -> None:
        writer = self._get_output_writer() if self.output_writer_factory is not None else None

        determinism = getattr(writer, "determinism", None)
        if determinism is None:
            determinism = self.determinism
        if determinism is None:
            determinism = DeterminismConfig()

        if writer is not None and format == "parquet":
            metadata = OutputMetadata.from_dataframe(
                df,
                run_id=writer.run_id,
                column_order=list(df.columns),
                hash_policy_version=getattr(determinism, "hash_policy_version", None),
                determinism=determinism,
            )
            writer.write(
                df,
                path,
                metadata=metadata,
                extended=False,
                apply_column_order=False,
            )
            return

        if format == "parquet":
            df.to_parquet(path, index=False)
            return

        if format == "csv":
            float_format = _resolve_float_format(determinism, None)
            date_format = _resolve_date_format(determinism, None)
            write_kwargs: dict[str, Any] = {"index": False}
            if float_format is not None:
                write_kwargs["float_format"] = float_format
            if date_format is not None:
                write_kwargs["date_format"] = date_format
            df.to_csv(path, **write_kwargs)
            return

        raise ValueError(f"Unsupported format: {format}")

    def _should_materialize(self, stage: str) -> bool:
        runtime = self.runtime
        if runtime is not None and bool(getattr(runtime, "dry_run", False)):
            logger.info("materialization_skipped", stage=stage, reason="dry_run")
            self._update_stage_context(stage, {"status": "skipped", "reason": "dry_run"})
            return False
        return True

    def _resolve_dataset_output_path(
        self,
        base_dir: Path,
        stage: str,
        dataset: str,
        format: str,
        *,
        fallback_stem: str,
    ) -> Path:
        configured = self.paths.resolve_dataset_path(stage, dataset, format)
        if configured is not None:
            return configured
        return self._resolve_sibling_path(base_dir, fallback_stem, format)

    @staticmethod
    def _resolve_sibling_path(directory: Path, stem: str, format: str) -> Path:
        return directory / f"{stem}{extension_for_format(format)}"

    @staticmethod
    def _resolve_default_stage_path(base: Path, format: str, *, fallback_stem: str | None = None) -> Path:
        base = Path(base)
        extension = extension_for_format(format)
        if base.suffix:
            stem = fallback_stem or base.stem
            return base.with_name(f"{stem}{extension}")
        stem = fallback_stem or base.name
        return base / f"{stem}{extension}"

    def _update_stage_context(self, stage: str, payload: dict[str, Any]) -> None:
        if self.stage_context is None:
            return
        materialization_ctx = self.stage_context.setdefault("materialization", {})
        stage_ctx = materialization_ctx.get(stage)
        if isinstance(stage_ctx, dict):
            stage_ctx.update(payload)
        else:
            materialization_ctx[stage] = payload.copy()

