"""Assay Pipeline - ChEMBL assay data extraction."""


import subprocess
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from bioetl.config import PipelineConfig
from bioetl.core.api_client import CircuitBreakerOpenError
from bioetl.core.logger import UnifiedLogger
from bioetl.normalizers import registry
from bioetl.pipelines.base import PipelineBase
from bioetl.schemas import AssaySchema
from bioetl.schemas.registry import schema_registry
from bioetl.utils.dataframe import resolve_schema_column_order
from bioetl.utils.dtypes import coerce_nullable_int, coerce_retry_after

from .client import AssayChEMBLClient
from .constants import (
    ASSAY_CLASS_ENRICHMENT_WHITELIST,
    NULLABLE_INT_COLUMNS,
    TARGET_ENRICHMENT_WHITELIST,
)
from .merge import AssayMergeService
from .normalizer import AssayNormalizer
from .output import AssayOutputWriter
from .parser import AssayParser
from .request import AssayRequestBuilder

__all__ = ["AssayPipeline"]

logger = UnifiedLogger.get(__name__)

# Register schema
schema_registry.register("assay", "1.0.0", AssaySchema)


# _coerce_nullable_int_columns заменена на coerce_nullable_int из bioetl.utils.dtypes


class AssayPipeline(PipelineBase):  # type: ignore[misc]
    """Pipeline for extracting ChEMBL assay data."""

    def __init__(self, config: PipelineConfig, run_id: str):
        super().__init__(config, run_id)
        self.primary_schema = AssaySchema

        default_base_url = "https://www.ebi.ac.uk/chembl/api/data"
        default_batch_size = 25
        default_max_url_length = 2000

        self.assay_client = AssayChEMBLClient(
            self,
            defaults={
                "enabled": True,
                "base_url": default_base_url,
                "batch_size": default_batch_size,
                "max_url_length": default_max_url_length,
            },
        )

        self.api_client = self.assay_client.api_client
        self.register_client(self.api_client)

        self.batch_size = self.assay_client.batch_size
        self.max_url_length = self.assay_client.max_url_length
        self.chembl_base_url = self.assay_client.base_url
        self.chembl_release: str | None = None
        self.git_commit = self._resolve_git_commit()
        self.config_hash = config.config_hash
        self.run_metadata: dict[str, Any] = {
            "run_id": run_id,
            "git_commit": self.git_commit,
            "config_hash": self.config_hash,
            "chembl_base_url": self.chembl_base_url,
            "chembl_release": None,
            "chembl_max_url_length": self.max_url_length,
        }

        self._status_payload: dict[str, Any] | None = None

        self.request_builder = AssayRequestBuilder(
            api_client=self.api_client,
            batch_size=self.batch_size,
            max_url_length=self.max_url_length,
        )
        self.parser = AssayParser()
        self.normalizer = AssayNormalizer()
        self.merge_service = AssayMergeService()
        self.output_writer = AssayOutputWriter(
            {
                "chembl_release": self.chembl_release,
                "run_id": self.run_id,
                "git_commit": self.git_commit,
                "config_hash": self.config_hash,
            }
        )

        self._initialize_status()

    def close_resources(self) -> None:
        """Освобождение локальных ресурсов пайплайна Assay."""

        if hasattr(self, "assay_client"):
            self.assay_client.clear_cache()

        if hasattr(self, "_status_payload"):
            self._status_payload = None

    @staticmethod
    def _resolve_git_commit() -> str:
        """Return the current git commit SHA or 'unknown'."""
        try:
            output = subprocess.check_output([
                "git",
                "rev-parse",
                "HEAD",
            ], stderr=subprocess.DEVNULL)
            return output.decode().strip()
        except (subprocess.CalledProcessError, FileNotFoundError, OSError):
            return "unknown"

    def _initialize_status(self) -> None:
        """Capture ChEMBL status to anchor the run's release metadata."""
        try:
            status = self.api_client.request_json("/status.json")
        except CircuitBreakerOpenError as exc:
            logger.error("chembl_status_circuit_open", error=str(exc))
            self.run_metadata["status_error"] = str(exc)
            return
        except Exception as exc:  # noqa: BLE001 - propagate as metadata for observability
            logger.warning("chembl_status_unavailable", error=str(exc))
            self.run_metadata["status_error"] = str(exc)
            return

        self._status_payload = status if isinstance(status, dict) else {}
        release: str | None = None
        if isinstance(status, dict):
            release_value = status.get("chembl_db_version") or status.get("chembl_release")
            if release_value:
                release = str(release_value)
        else:
            logger.warning(
                "chembl_status_unexpected_payload",
                payload_type=type(status).__name__,
            )

        if release:
            self.chembl_release = release
            self.run_metadata["chembl_release"] = release
            self.output_writer.update_release(release)
            logger.info("chembl_status_captured", chembl_release=release, base_url=self.chembl_base_url)
        else:
            logger.warning("chembl_status_missing_release", base_url=self.chembl_base_url)

        self.run_metadata["status_checked_at"] = datetime.now(timezone.utc).isoformat()

    def _make_cache_key(self, assay_id: str) -> str:
        """Compose release-qualified cache key for assay payloads."""
        release = self.chembl_release or "unknown"
        return f"assay:{release}:{assay_id}"

    def _update_fetch_metrics(self) -> None:
        """Persist aggregated fetch metrics for observability and QC."""

        metrics_payload = self.assay_client.snapshot_metrics()
        success_count = int(metrics_payload.get("success_count", 0))
        fallback_total = int(metrics_payload.get("fallback_total", 0))
        cache_hits = int(metrics_payload.get("cache_hits", 0))
        cache_fallback_hits = int(metrics_payload.get("cache_fallback_hits", 0))

        logger.info("assay_fetch_metrics", **metrics_payload)

        self.run_metadata["assay_fetch_metrics"] = metrics_payload
        self.qc_metrics["assay_fetch_success_count"] = success_count
        self.qc_metrics["assay_fallback_total"] = fallback_total
        self.qc_metrics["assay_fetch_cache_hits"] = cache_hits
        self.qc_metrics["assay_fetch_cache_fallback_hits"] = cache_fallback_hits

    def extract(self, input_file: Path | None = None) -> pd.DataFrame:
        """Extract assay data from input file."""
        df, resolved_path = self.read_input_table(
            default_filename=Path("assay.csv"),
            expected_columns=["assay_chembl_id"],
            dtype="string",
            input_file=input_file,
        )

        if not resolved_path.exists():
            return df

        result = pd.DataFrame({"assay_chembl_id": df.get("assay_chembl_id", pd.Series(dtype="string"))})

        logger.info("extraction_completed", rows=len(result))
        return result

    def _fetch_assay_data(self, assay_ids: list[str]) -> pd.DataFrame:
        """Fetch assay data from ChEMBL API with release-scoped caching."""

        if not assay_ids:
            return pd.DataFrame()

        records = self.assay_client.fetch_assays(
            assay_ids,
            release=self.chembl_release,
            request_builder=self.request_builder,
            transform=lambda payload: self.normalizer.normalize_assay(
                payload,
                self.chembl_release,
            ),
            fallback_factory=self.output_writer.register_fallback,
        )

        if not records:
            logger.warning("no_results_from_api")
            self._update_fetch_metrics()
            return pd.DataFrame()

        df = pd.DataFrame(records)
        logger.info("api_extraction_completed", rows=len(df))
        self._update_fetch_metrics()
        return df

    def _expand_assay_parameters_long(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compatibility shim delegating to the dedicated parser."""

        return self.parser.expand_parameters(df)

    # NOTE:
    #     Variant sequence details remain embedded within the
    #     ``variant_sequence_json`` column of the base assay row. Expanding
    #     these entries into dedicated ``row_subtype="variant"`` records led to
    #     duplicate assay rows after concatenation. The helper below is kept as
    #     historical reference but intentionally disabled.
    # def _expand_variant_sequences(self, df: pd.DataFrame) -> pd.DataFrame:
    #     """Expand variant sequences JSON into long-format rows."""
    #
    #     if "variant_sequence_json" not in df.columns:
    #         return pd.DataFrame(columns=["assay_chembl_id", "row_subtype", "row_index"])
    #
    #     records: list[dict[str, object]] = []
    #
    #     for _, row in df.iterrows():
    #         variant_raw = row.get("variant_sequence_json")
    #         if not variant_raw or (isinstance(variant_raw, float) and pd.isna(variant_raw)):
    #             continue
    #
    #         variants = variant_raw if isinstance(variant_raw, Iterable) else None
    #         if variants is None:
    #             continue
    #
    #         if not isinstance(variants, Iterable):
    #             continue
    #
    #         index = 0
    #         for variant in variants:
    #             if not isinstance(variant, dict):
    #                 continue
    #
    #             record = {
    #                 "assay_chembl_id": row.get("assay_chembl_id"),
    #                 "row_subtype": "variant",
    #                 "row_index": index,
    #                 "variant_id": variant.get("variant_id"),
    #                 "variant_base_accession": variant.get("base_accession"),
    #                 "variant_mutation": variant.get("mutation"),
    #                 "variant_sequence": variant.get("variant_seq"),
    #                 "variant_accession_reported": variant.get("accession_reported"),
    #             }
    #             records.append(record)
    #             index += 1
    #
    #     if not records:
    #         return pd.DataFrame(columns=["assay_chembl_id", "row_subtype", "row_index"])
    #
    #     return pd.DataFrame(records)

    def _expand_assay_classifications(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compatibility shim retained for legacy callers."""

        return self.parser.expand_classifications(df)

    def _fetch_target_reference_data(self, target_ids: list[str]) -> pd.DataFrame:
        """Fetch whitelisted target reference data for enrichment."""

        if not target_ids:
            return pd.DataFrame(columns=TARGET_ENRICHMENT_WHITELIST)

        records: list[dict[str, object]] = []

        for target_id in sorted(set(filter(None, target_ids))):
            try:
                url = f"{self.api_client.config.base_url}/target/{target_id}.json"
                response = self.api_client.request_json(url)
                target_data = response.get("target") if isinstance(response, dict) and "target" in response else response

                if not isinstance(target_data, dict):
                    continue

                record = {
                    field: target_data.get(field)
                    for field in TARGET_ENRICHMENT_WHITELIST
                }
                records.append(record)
            except Exception as exc:
                logger.warning("target_enrichment_failed", target_id=target_id, error=str(exc))

        if not records:
            return pd.DataFrame(columns=TARGET_ENRICHMENT_WHITELIST)

        return pd.DataFrame(records)

    def _fetch_assay_class_reference_data(self, class_ids: Iterable[int | str]) -> pd.DataFrame:
        """Fetch whitelisted assay class reference data for enrichment."""

        normalized_ids = [class_id for class_id in sorted(set(class_ids)) if class_id is not None]
        if not normalized_ids:
            return pd.DataFrame(columns=ASSAY_CLASS_ENRICHMENT_WHITELIST.values())

        records: list[dict[str, object]] = []

        for class_id in normalized_ids:
            try:
                url = f"{self.api_client.config.base_url}/assay_class/{class_id}.json"
                response = self.api_client.request_json(url)
                class_data = (
                    response.get("assay_class")
                    if isinstance(response, dict) and "assay_class" in response
                    else response
                )

                if not isinstance(class_data, dict):
                    continue

                record = {
                    output_field: class_data.get(input_field)
                    for input_field, output_field in ASSAY_CLASS_ENRICHMENT_WHITELIST.items()
                }
                records.append(record)
            except Exception as exc:
                logger.warning("assay_class_enrichment_failed", assay_class_id=class_id, error=str(exc))

        output_columns = list(ASSAY_CLASS_ENRICHMENT_WHITELIST.values())
        if not records:
            return pd.DataFrame(columns=output_columns)

        return pd.DataFrame(records)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform assay data and expand nested parameter/class payloads."""
        from bioetl.schemas.assay import AssaySchema

        if df.empty:
            # Return empty DataFrame with all required columns from schema
            return pd.DataFrame(columns=resolve_schema_column_order(AssaySchema))

        # Fetch assay data from ChEMBL API
        assay_ids = df["assay_chembl_id"].unique().tolist()
        assay_data = self._fetch_assay_data(assay_ids)

        # Merge with existing data
        if not assay_data.empty:
            df = df.merge(assay_data, on="assay_chembl_id", how="left", suffixes=("", "_api"))
            # Remove duplicate columns from API merge (keep original, remove _api suffix)
            df = df.loc[:, ~df.columns.str.endswith("_api")]

        # Normalize strings
        if "assay_description" in df.columns:
            df["assay_description"] = df["assay_description"].apply(
                lambda x: registry.normalize("string", x) if pd.notna(x) else None
            )

        if "assay_type" in df.columns:
            df["assay_type"] = df["assay_type"].apply(
                lambda x: registry.normalize("string", x) if pd.notna(x) else None
            )

        base_df = df.copy()
        base_df["row_subtype"] = "assay"
        base_df["row_index"] = 0
        base_df = self.output_writer.materialize_base(base_df)

        target_ids_series = base_df.get("target_chembl_id")
        if target_ids_series is not None:
            target_id_list = [tid for tid in target_ids_series.dropna().tolist() if tid]
            if target_id_list:
                target_reference = self._fetch_target_reference_data(target_id_list)
                base_df = self.normalizer.enrich_targets(base_df, target_reference)

        params_df = self.output_writer.materialize_parameters(
            self.parser.expand_parameters(base_df)
        )

        classes_df = self.parser.expand_classifications(base_df)
        if not classes_df.empty and "assay_class_id" in classes_df.columns:
            class_ids = [cid for cid in classes_df["assay_class_id"].dropna().tolist() if cid is not None]
            if class_ids:
                class_reference = self._fetch_assay_class_reference_data(class_ids)
                classes_df = self.normalizer.enrich_assay_classes(classes_df, class_reference)

        classes_df = self.output_writer.materialize_classifications(classes_df)

        df = self.merge_service.merge_frames(base_df, params_df, classes_df)

        # Normalise nullable integer columns to Pandas' nullable Int64 dtype so
        # Pandera can coerce them into the expected dtype('int64') during
        # validation. Mixed object/float columns originating from API payloads
        # previously triggered schema validation errors because Pandera refuses
        # to coerce values like "501" or NaN into strict integers when the
        # series dtype is ``object``. Explicitly converting here keeps the data
        # model consistent across all row subtypes.
        if "row_index" in df.columns:
            df["row_index"] = df["row_index"].fillna(0).astype("Int64")
        else:
            df["row_index"] = pd.Series([0] * len(df), dtype="Int64")

        nullable_int_columns = list(NULLABLE_INT_COLUMNS)

        coerce_nullable_int(df, nullable_int_columns)

        pipeline_version = self.config.pipeline.version
        default_source = "chembl"

        release_value: str | None = self.chembl_release
        if isinstance(release_value, str):
            release_value = release_value.strip() or None

        df = self.finalize_with_standard_metadata(
            df,
            business_key="assay_chembl_id",
            sort_by=["assay_chembl_id", "row_subtype", "row_index"],
            ascending=[True, True, True],
            schema=AssaySchema,
            default_source=default_source,
            chembl_release=release_value,
        )

        coerce_nullable_int(df, nullable_int_columns)
        coerce_retry_after(df)

        return df

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate assay data against schema and referential integrity."""
        from bioetl.schemas.assay import AssaySchema

        self.validation_issues.clear()

        if df.empty:
            logger.info("validation_skipped_empty", rows=0)
            return df

        canonical_order = resolve_schema_column_order(AssaySchema)
        if canonical_order:
            missing_columns = [column for column in canonical_order if column not in df.columns]
            for column in missing_columns:
                df[column] = pd.NA

            extra_columns = [column for column in df.columns if column not in canonical_order]
            if extra_columns:
                df = df.drop(columns=extra_columns)

            df = df.loc[:, canonical_order]
        else:  # pragma: no cover - defensive fallback
            schema_columns = list(AssaySchema.to_schema().columns.keys())
            if schema_columns:
                for column in schema_columns:
                    if column not in df.columns:
                        df[column] = pd.NA
                df = df.loc[:, schema_columns]

        coerce_retry_after(df)
        # Normalise nullable integer columns once more before validation.
        #
        # Even though ``transform`` already coerces these columns, downstream
        # callers (including CLI sampling) may mutate frames in between the
        # stages.  Pandera raises ``coerce_dtype('int64')`` errors when a single
        # fractional value (e.g. ``"3.5"``) slips through, so we defensively
        # reuse the same normalisation helper to guarantee determinism at the
        # point of validation.
        coerce_nullable_int(df, NULLABLE_INT_COLUMNS)

        def _assay_error_adapter(
            issues: list[dict[str, Any]],
            exc: Exception,
            should_fail: bool,
        ) -> Exception | None:
            if not should_fail:
                return None

            if not issues:
                return ValueError("Schema validation failed")

            summary = "; ".join(
                f"{issue.get('column')}: {issue.get('check')} ({issue.get('count')} cases)"
                for issue in issues
            )
            return ValueError(f"Schema validation failed: {summary}")

        validated_df = self.run_schema_validation(
            df,
            AssaySchema,
            dataset_name="assays",
            severity="error",
            metric_name="schema.validation",
            error_adapter=_assay_error_adapter,
        )

        output_order = resolve_schema_column_order(AssaySchema)
        if output_order:
            missing_output = [col for col in output_order if col not in validated_df.columns]
            if missing_output:  # pragma: no cover - defensive
                for column in missing_output:
                    validated_df[column] = pd.NA
            validated_df = validated_df.reindex(columns=output_order)

        self._check_referential_integrity(validated_df)

        logger.info(
            "validation_completed",
            rows=len(validated_df),
            issues=len(self.validation_issues),
        )
        return validated_df

    def _load_target_reference_ids(self) -> set[str]:
        """Load known target identifiers for referential integrity checks."""

        target_path = Path(self.config.paths.input_root) / "target.csv"
        if not target_path.exists():
            logger.warning("referential_check_skipped", reason="target_file_missing", path=str(target_path))
            return set()

        try:
            target_df = pd.read_csv(target_path, usecols=["target_chembl_id"])
        except ValueError:
            logger.warning(
                "referential_check_skipped",
                reason="target_column_missing",
                path=str(target_path),
            )
            return set()
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning(
                "referential_check_skipped",
                reason="target_load_failed",
                error=str(exc),
                path=str(target_path),
            )
            return set()

        values = (
            target_df["target_chembl_id"]
            .apply(
                lambda raw: (
                    registry.normalize("chemistry.chembl_id", raw)
                    if pd.notna(raw)
                    else None
                )
            )
            .dropna()
        )
        reference_ids = {value for value in values.tolist() if value}
        logger.debug(
            "referential_reference_loaded",
            path=str(target_path),
            count=len(reference_ids),
        )
        return reference_ids

    def _check_referential_integrity(self, df: pd.DataFrame) -> None:
        """Validate assay → target relationships against reference data."""

        if "target_chembl_id" not in df.columns:
            logger.debug("referential_check_skipped", reason="column_absent")
            return

        reference_ids = self._load_target_reference_ids()
        if not reference_ids:
            return

        target_series = df["target_chembl_id"].apply(
            lambda raw: (
                registry.normalize("chemistry.chembl_id", raw)
                if pd.notna(raw)
                else None
            )
        )
        missing_mask = target_series.notna() & ~target_series.isin(reference_ids)
        missing_count = int(missing_mask.sum())

        if missing_count == 0:
            logger.info("referential_integrity_passed", relation="assay->target")
            return

        total_rows = len(df)
        missing_ratio = missing_count / total_rows if total_rows else 0.0
        threshold = float(self.config.qc.thresholds.get("assay.target_missing_ratio", 0.0))
        severity = "error" if missing_ratio > threshold else "info"

        sample_targets = (
            target_series[missing_mask]
            .dropna()
            .unique()
            .tolist()[:5]
        )

        issue = {
            "issue_type": "referential_integrity",
            "severity": severity,
            "column": "target_chembl_id",
            "check": "assay->target",
            "count": missing_count,
            "ratio": missing_ratio,
            "threshold": threshold,
            "details": ", ".join(sample_targets),
        }
        self.record_validation_issue(issue)

        log_fn = logger.error if severity == "error" else logger.warning
        log_fn(
            "referential_integrity_failure",
            relation="assay->target",
            missing_count=missing_count,
            missing_ratio=missing_ratio,
            threshold=threshold,
            severity=severity,
        )

        if self._should_fail(severity):
            raise ValueError(
                "Referential integrity violation: assays reference missing targets"
            )
