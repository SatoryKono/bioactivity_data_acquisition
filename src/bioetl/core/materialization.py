"""Helpers for deterministic materialization of pipeline artefacts."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, MutableMapping

import pandas as pd

from bioetl.config.models import MaterializationPaths
from bioetl.core.logger import UnifiedLogger
from bioetl.core.output_writer import OutputMetadata, UnifiedOutputWriter

logger = UnifiedLogger.get(__name__)

DEFAULT_SILVER_PATH = Path("data/output/target/targets_silver.parquet")
DEFAULT_GOLD_PATH = Path("data/output/target/targets_final.parquet")


@dataclass
class MaterializationManager:
    """Coordinate writing of pipeline artefacts with deterministic behaviour."""

    paths: MaterializationPaths
    runtime: Any | None = None
    stage_context: MutableMapping[str, Any] | None = None
    writer: UnifiedOutputWriter | None = None

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

        resolved_format = self._normalise_format(format)
        configured = output_path if output_path is not None else self.paths.silver
        base_path = self._resolve_configured_path(configured, DEFAULT_SILVER_PATH)
        silver_path = self._resolve_dataset_path(base_path, "targets_silver", resolved_format)
        silver_path.parent.mkdir(parents=True, exist_ok=True)

        outputs: dict[str, Path] = {}

        if not uniprot_df.empty:
            sorted_uniprot = uniprot_df.sort_values("canonical_accession", kind="stable")
            if self.writer is not None:
                bundle = self.writer.write_table_bundle(
                    sorted_uniprot,
                    silver_path,
                    formats=(resolved_format,),
                    metadata=OutputMetadata.from_dataframe(
                        sorted_uniprot,
                        run_id=self.writer.run_id,
                        column_order=list(sorted_uniprot.columns),
                    ),
                    qc_directory=silver_path.parent / "qc",
                )
                dataset_path = bundle.datasets.get(
                    resolved_format,
                    next(iter(bundle.datasets.values())),
                )
                logger.info(
                    "writing_silver_dataset",
                    path=str(dataset_path),
                    rows=len(sorted_uniprot),
                    format=resolved_format,
                )
                outputs["uniprot"] = dataset_path
            else:
                logger.info(
                    "writing_silver_dataset",
                    path=str(silver_path),
                    rows=len(sorted_uniprot),
                )
                self._write_dataframe(sorted_uniprot, silver_path, resolved_format)
                outputs["uniprot"] = silver_path

        component_path = self._resolve_sibling_path(
            silver_path.parent,
            "component_enrichment",
            resolved_format,
        )
        if not component_df.empty:
            sorted_components = component_df.sort_values(
                ["canonical_accession", "isoform_accession"],
                kind="stable",
            )
            if self.writer is not None:
                bundle = self.writer.write_table_bundle(
                    sorted_components,
                    component_path,
                    formats=(resolved_format,),
                    metadata=OutputMetadata.from_dataframe(
                        sorted_components,
                        run_id=self.writer.run_id,
                        column_order=list(sorted_components.columns),
                    ),
                    qc_directory=component_path.parent / "qc",
                )
                dataset_path = bundle.datasets.get(
                    resolved_format,
                    next(iter(bundle.datasets.values())),
                )
                logger.info(
                    "writing_component_enrichment",
                    path=str(dataset_path),
                    rows=len(sorted_components),
                    format=resolved_format,
                )
                outputs["component_enrichment"] = dataset_path
            else:
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

        resolved_format = self._normalise_format(format)
        configured = output_directory if output_directory is not None else self.paths.gold
        gold_base = self._resolve_configured_path(configured, DEFAULT_GOLD_PATH)
        output_dir = gold_base.parent if gold_base.suffix else gold_base
        output_dir.mkdir(parents=True, exist_ok=True)

        outputs: dict[str, Path] = {}

        if not classification_df.empty:
            classification_path = self._resolve_sibling_path(
                output_dir,
                "targets_iuphar_classification",
                resolved_format,
            )
            sorted_classification = classification_df.sort_values(
                ["target_chembl_id", "iuphar_family_id"],
                kind="stable",
            )
            if self.writer is not None:
                bundle = self.writer.write_table_bundle(
                    sorted_classification,
                    classification_path,
                    formats=(resolved_format,),
                    metadata=OutputMetadata.from_dataframe(
                        sorted_classification,
                        run_id=self.writer.run_id,
                        column_order=list(sorted_classification.columns),
                    ),
                    qc_directory=classification_path.parent / "qc",
                )
                dataset_path = bundle.datasets.get(
                    resolved_format,
                    next(iter(bundle.datasets.values())),
                )
                logger.info(
                    "writing_iuphar_classification",
                    path=str(dataset_path),
                    rows=len(sorted_classification),
                    format=resolved_format,
                )
                outputs["classification"] = dataset_path
            else:
                logger.info(
                    "writing_iuphar_classification",
                    path=str(classification_path),
                    rows=len(sorted_classification),
                )
                self._write_dataframe(sorted_classification, classification_path, resolved_format)
                outputs["classification"] = classification_path

        if not gold_df.empty:
            gold_path = self._resolve_sibling_path(
                output_dir,
                "targets_iuphar_enrichment",
                resolved_format,
            )
            sorted_gold = gold_df.sort_values(["target_chembl_id"], kind="stable")
            if self.writer is not None:
                bundle = self.writer.write_table_bundle(
                    sorted_gold,
                    gold_path,
                    formats=(resolved_format,),
                    metadata=OutputMetadata.from_dataframe(
                        sorted_gold,
                        run_id=self.writer.run_id,
                        column_order=list(sorted_gold.columns),
                    ),
                    qc_directory=gold_path.parent / "qc",
                )
                dataset_path = bundle.datasets.get(
                    resolved_format,
                    next(iter(bundle.datasets.values())),
                )
                logger.info(
                    "writing_iuphar_gold",
                    path=str(dataset_path),
                    rows=len(sorted_gold),
                    format=resolved_format,
                )
                outputs["iuphar_gold"] = dataset_path
            else:
                logger.info(
                    "writing_iuphar_gold",
                    path=str(gold_path),
                    rows=len(sorted_gold),
                )
                self._write_dataframe(sorted_gold, gold_path, resolved_format)
                outputs["iuphar_gold"] = gold_path

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

        resolved_format = self._normalise_format(format)
        configured = output_path if output_path is not None else self.paths.gold
        base_path = self._resolve_configured_path(configured, DEFAULT_GOLD_PATH)

        if base_path.suffix:
            base_dir = base_path.parent
            targets_path = base_path.with_suffix(self._extension_for(resolved_format))
        else:
            base_dir = base_path
            targets_path = self._resolve_sibling_path(base_dir, "targets", resolved_format)

        base_dir.mkdir(parents=True, exist_ok=True)

        outputs: dict[str, Path] = {}

        outputs.update(
            self._materialize_stage_dataset(
                targets,
                targets_path,
                resolved_format,
                label="targets",
            )
        )

        outputs.update(
            self._materialize_stage_dataset(
                components,
                self._resolve_sibling_path(base_dir, "target_components", resolved_format),
                resolved_format,
                label="target_components",
            )
        )
        outputs.update(
            self._materialize_stage_dataset(
                protein_class,
                self._resolve_sibling_path(base_dir, "protein_class", resolved_format),
                resolved_format,
                label="protein_class",
            )
        )
        outputs.update(
            self._materialize_stage_dataset(
                xref,
                self._resolve_sibling_path(base_dir, "target_xref", resolved_format),
                resolved_format,
                label="target_xref",
            )
        )

        if outputs:
            self._update_stage_context("gold", {"outputs": outputs})

        return outputs

    # Internal helpers -------------------------------------------------

    def _materialize_stage_dataset(
        self,
        df: pd.DataFrame,
        path: Path,
        format: str,
        *,
        label: str,
    ) -> dict[str, Path]:
        if df.empty:
            logger.info("gold_materialization_skipped", dataset=label, reason="empty_dataframe")
            return {}

        if self.writer is not None:
            metadata = OutputMetadata.from_dataframe(
                df,
                run_id=self.writer.run_id,
                column_order=list(df.columns),
            )
            bundle = self.writer.write_table_bundle(
                df,
                path,
                formats=(format,),
                metadata=metadata,
                qc_directory=path.parent / "qc",
            )
            dataset_path = bundle.datasets.get(format, next(iter(bundle.datasets.values())))
            logger.info(
                "writing_gold_dataset",
                dataset=label,
                path=str(dataset_path),
                rows=len(df),
                format=format,
            )
        else:
            logger.info("writing_gold_dataset", dataset=label, path=str(path), rows=len(df))
            self._write_dataframe(df, path, format)
            dataset_path = path

        return {label: dataset_path}

    def _write_dataframe(self, df: pd.DataFrame, path: Path, format: str) -> None:
        if format == "parquet":
            df.to_parquet(path, index=False)
        elif format == "csv":
            df.to_csv(path, index=False)
        else:  # pragma: no cover - defensive guard
            raise ValueError(f"Unsupported format: {format}")

    def _should_materialize(self, stage: str) -> bool:
        runtime = self.runtime
        if runtime is not None and bool(getattr(runtime, "dry_run", False)):
            logger.info("materialization_skipped", stage=stage, reason="dry_run")
            self._update_stage_context(stage, {"status": "skipped", "reason": "dry_run"})
            return False
        return True

    @staticmethod
    def _resolve_configured_path(value: Path | str | None, default: Path) -> Path:
        if value is None:
            return default
        return Path(value)

    @staticmethod
    def _resolve_dataset_path(base: Path, stem: str, format: str) -> Path:
        if base.suffix:
            return base.with_suffix(MaterializationManager._extension_for(format))
        return base / f"{stem}{MaterializationManager._extension_for(format)}"

    @staticmethod
    def _resolve_sibling_path(directory: Path, stem: str, format: str) -> Path:
        return directory / f"{stem}{MaterializationManager._extension_for(format)}"

    @staticmethod
    def _extension_for(format: str) -> str:
        if format == "parquet":
            return ".parquet"
        if format == "csv":
            return ".csv"
        raise ValueError(f"Unsupported format: {format}")

    @staticmethod
    def _normalise_format(format: str) -> str:
        value = format.lower().strip()
        if value not in {"csv", "parquet"}:
            raise ValueError(f"Unsupported materialization format: {format}")
        return value

    def _update_stage_context(self, stage: str, payload: dict[str, Any]) -> None:
        if self.stage_context is None:
            return
        materialization_ctx = self.stage_context.setdefault("materialization", {})
        stage_ctx = materialization_ctx.get(stage)
        if isinstance(stage_ctx, dict):
            stage_ctx.update(payload)
        else:
            materialization_ctx[stage] = payload.copy()

