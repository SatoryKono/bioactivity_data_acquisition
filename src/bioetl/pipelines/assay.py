"""Assay Pipeline - ChEMBL assay data extraction."""

import json
import subprocess
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests
from pandera.errors import SchemaErrors

from bioetl.config import PipelineConfig
from bioetl.core.api_client import APIConfig, CircuitBreakerOpenError, UnifiedAPIClient
from bioetl.core.logger import UnifiedLogger
from bioetl.pipelines.base import PipelineBase
from bioetl.schemas import AssaySchema
from bioetl.schemas.registry import schema_registry

logger = UnifiedLogger.get(__name__)

# Register schema
schema_registry.register("assay", "1.0.0", AssaySchema)


TARGET_ENRICHMENT_WHITELIST = [
    "target_chembl_id",
    "pref_name",
    "organism",
    "target_type",
    "species_group_flag",
    "tax_id",
    "component_count",
]

ASSAY_CLASS_ENRICHMENT_WHITELIST = {
    "assay_class_id": "assay_class_id",
    "bao_id": "assay_class_bao_id",
    "class_type": "assay_class_type",
    "l1": "assay_class_l1",
    "l2": "assay_class_l2",
    "l3": "assay_class_l3",
    "description": "assay_class_description",
}


# Nullable integer columns that require explicit coercion before schema validation.
_NULLABLE_INT_COLUMNS = (
    "row_index",
    "assay_tax_id",
    "confidence_score",
    "species_group_flag",
    "tax_id",
    "component_count",
    "assay_class_id",
    "variant_id",
    "src_id",
)

_INT64_LIMITS = np.iinfo(np.int64)


def _coerce_nullable_int_columns(df: pd.DataFrame, columns: Iterable[str]) -> None:
    """Coerce selected columns to pandas nullable Int64 dtype."""

    for column in columns:
        if column not in df.columns:
            continue

        numeric = pd.to_numeric(df[column], errors="coerce")
        numeric = pd.Series(numeric, index=df.index, dtype="Float64")

        finite_mask = np.isfinite(numeric.to_numpy(dtype=float, copy=False))
        numeric = numeric.mask(~finite_mask, pd.NA)

        valid_mask = numeric.notna()
        if valid_mask.any():
            fractional_mask = np.zeros(len(numeric), dtype=bool)
            fractional_values = np.modf(numeric[valid_mask].to_numpy(dtype=float, copy=False))[0]
            if fractional_values.size:
                non_integer = np.abs(fractional_values) > np.finfo(float).eps
                if np.any(non_integer):
                    fractional_mask[valid_mask.to_numpy(copy=False)] = non_integer
                    numeric = numeric.mask(fractional_mask, pd.NA)
                    valid_mask = numeric.notna()

        if valid_mask.any():
            overflow_mask = np.zeros(len(numeric), dtype=bool)
            constrained_values = numeric[valid_mask].to_numpy(dtype=float, copy=False)
            if constrained_values.size:
                exceeds = (constrained_values > _INT64_LIMITS.max) | (
                    constrained_values < _INT64_LIMITS.min
                )
                if np.any(exceeds):
                    overflow_mask[valid_mask.to_numpy(copy=False)] = exceeds
                    numeric = numeric.mask(overflow_mask, pd.NA)

        df[column] = pd.Series(pd.array(numeric, dtype="Int64"), index=df.index)
        logger.debug(
            "nullable_int_coerced",
            column=column,
            dtype=str(df[column].dtype),
            non_null=int(df[column].notna().sum()),
        )


class AssayPipeline(PipelineBase):
    """Pipeline for extracting ChEMBL assay data."""

    def __init__(self, config: PipelineConfig, run_id: str):
        super().__init__(config, run_id)

        chembl_source = config.sources.get("chembl")
        default_base_url = "https://www.ebi.ac.uk/chembl/api/data"
        base_url = getattr(chembl_source, "base_url", default_base_url)
        batch_size = getattr(chembl_source, "batch_size", 25) or 25

        chembl_config = APIConfig(
            name="chembl",
            base_url=base_url,
            cache_enabled=config.cache.enabled,
            cache_ttl=config.cache.ttl,
        )
        self.api_client = UnifiedAPIClient(chembl_config)

        self.batch_size = batch_size
        self.chembl_base_url = base_url
        self.chembl_release: str | None = None
        self.git_commit = self._resolve_git_commit()
        self.config_hash = config.config_hash
        self.run_metadata: dict[str, Any] = {
            "run_id": run_id,
            "git_commit": self.git_commit,
            "config_hash": self.config_hash,
            "chembl_base_url": self.chembl_base_url,
            "chembl_release": None,
        }

        self._cache_enabled = bool(config.cache.enabled)
        self._assay_cache: dict[str, dict[str, Any]] = {}
        self._status_payload: dict[str, Any] | None = None

        self._initialize_status()

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
            logger.info("chembl_status_captured", chembl_release=release, base_url=self.chembl_base_url)
        else:
            logger.warning("chembl_status_missing_release", base_url=self.chembl_base_url)

        self.run_metadata["status_checked_at"] = datetime.now(timezone.utc).isoformat()

    def _make_cache_key(self, assay_id: str) -> str:
        """Compose release-qualified cache key for assay payloads."""
        release = self.chembl_release or "unknown"
        return f"assay:{release}:{assay_id}"

    def _cache_get(self, assay_id: str) -> dict[str, Any] | None:
        """Return cached assay payload if caching is enabled."""
        if not self._cache_enabled:
            return None
        key = self._make_cache_key(assay_id)
        cached = self._assay_cache.get(key)
        if cached is not None:
            logger.debug("assay_cache_hit", key=key)
            return cached.copy()
        return None

    def _cache_set(self, assay_id: str, payload: dict[str, Any]) -> None:
        """Persist payload to in-memory cache if enabled."""
        if not self._cache_enabled:
            return
        key = self._make_cache_key(assay_id)
        self._assay_cache[key] = payload.copy()
        logger.debug("assay_cache_store", key=key)

    def _normalize_assay_record(self, assay: dict[str, Any]) -> dict[str, Any]:
        """Normalize raw assay payload from ChEMBL."""
        classifications = assay.get("assay_classifications")
        classifications_str = None
        if classifications:
            try:
                classifications_str = json.dumps(
                    classifications,
                    ensure_ascii=False,
                    sort_keys=True,
                )
            except (TypeError, ValueError):
                try:
                    classifications_str = " | ".join(classifications)
                except TypeError:
                    classifications_str = None

        params = assay.get("assay_parameters")
        params_json = json.dumps(params, ensure_ascii=False) if params is not None else None

        record: dict[str, Any] = {
            "assay_chembl_id": assay.get("assay_chembl_id"),
            "assay_type": assay.get("assay_type"),
            "assay_category": assay.get("assay_category"),
            "assay_cell_type": assay.get("assay_cell_type"),
            "assay_classifications": classifications_str,
            "assay_group": assay.get("assay_group"),
            "assay_organism": assay.get("assay_organism"),
            "assay_parameters_json": params_json,
            "assay_strain": assay.get("assay_strain"),
            "assay_subcellular_fraction": assay.get("assay_subcellular_fraction"),
            "assay_tax_id": assay.get("assay_tax_id"),
            "assay_test_type": assay.get("assay_test_type"),
            "assay_tissue": assay.get("assay_tissue"),
            "assay_type_description": assay.get("assay_type_description"),
            "bao_format": assay.get("bao_format"),
            "bao_label": assay.get("bao_label"),
            "bao_endpoint": assay.get("bao_endpoint"),
            "cell_chembl_id": assay.get("cell_chembl_id"),
            "confidence_description": assay.get("confidence_description"),
            "confidence_score": assay.get("confidence_score"),
            "assay_description": assay.get("description"),
            "document_chembl_id": assay.get("document_chembl_id"),
            "relationship_description": assay.get("relationship_description"),
            "relationship_type": assay.get("relationship_type"),
            "src_assay_id": assay.get("src_assay_id"),
            "src_id": assay.get("src_id"),
            "target_chembl_id": assay.get("target_chembl_id"),
            "tissue_chembl_id": assay.get("tissue_chembl_id"),
        }

        assay_class = assay.get("assay_class")
        if isinstance(assay_class, dict):
            record.update({
                "assay_class_id": assay_class.get("assay_class_id"),
                "assay_class_bao_id": assay_class.get("bao_id"),
                "assay_class_type": assay_class.get("assay_class_type"),
                "assay_class_l1": assay_class.get("class_level_1"),
                "assay_class_l2": assay_class.get("class_level_2"),
                "assay_class_l3": assay_class.get("class_level_3"),
                "assay_class_description": assay_class.get("assay_class_description"),
            })

        variant_sequences = assay.get("variant_sequence")
        variant_sequence_json = None
        if variant_sequences:
            if isinstance(variant_sequences, list) and variant_sequences:
                variant = variant_sequences[0]
                if isinstance(variant, dict):
                    record.update({
                        "variant_id": variant.get("variant_id"),
                        "variant_base_accession": variant.get("base_accession"),
                        "variant_mutation": variant.get("mutation"),
                        "variant_sequence": variant.get("variant_seq"),
                        "variant_accession_reported": variant.get("accession_reported"),
                    })
                variant_sequence_json = json.dumps(variant_sequences, ensure_ascii=False)
            elif isinstance(variant_sequences, dict):
                record.update({
                    "variant_id": variant_sequences.get("variant_id"),
                    "variant_base_accession": variant_sequences.get("base_accession"),
                    "variant_mutation": variant_sequences.get("mutation"),
                    "variant_sequence": variant_sequences.get("variant_seq"),
                    "variant_accession_reported": variant_sequences.get("accession_reported"),
                })
                variant_sequence_json = json.dumps([variant_sequences], ensure_ascii=False)

        record["variant_sequence_json"] = variant_sequence_json
        record["source_system"] = "chembl"
        record["chembl_release"] = self.chembl_release
        return record

    def _create_fallback_record(self, assay_id: str, error: Exception | None = None) -> dict[str, Any]:
        """Generate fallback payload when API data cannot be retrieved."""
        http_status: int | None = None
        retry_after: float | None = None
        error_code: Any | None = getattr(error, "code", None)
        attempt: Any | None = getattr(error, "attempt", None)

        if isinstance(error, requests.exceptions.HTTPError) and error.response is not None:
            http_status = error.response.status_code
            retry_after_header = error.response.headers.get("Retry-After")
            if retry_after_header:
                try:
                    retry_after = float(retry_after_header)
                except ValueError:
                    retry_after = None
        else:
            http_status = getattr(error, "status", None)
            retry_after = getattr(error, "retry_after", None)

        message = str(error) if error else "Fallback: API unavailable"

        record: dict[str, Any] = {
            "assay_chembl_id": assay_id,
            "source_system": "ChEMBL_FALLBACK",
            "chembl_release": self.chembl_release,
            "error_code": error_code,
            "http_status": http_status,
            "error_message": message,
            "retry_after_sec": retry_after,
            "attempt": attempt,
            "run_id": self.run_id,
            "git_commit": self.git_commit,
            "config_hash": self.config_hash,
        }
        return record

    def extract(self, input_file: Path | None = None) -> pd.DataFrame:
        """Extract assay data from input file."""
        if input_file is None:
            # Default to data/input/assay.csv
            input_file = Path("data/input/assay.csv")

        logger.info("reading_input", path=input_file)
        df = pd.read_csv(input_file)  # Read all records

        # Create output DataFrame with just assay_chembl_id for API lookup
        result = pd.DataFrame()
        result["assay_chembl_id"] = df["assay_chembl_id"] if "assay_chembl_id" in df.columns else df["assay_chembl_id"]

        logger.info("extraction_completed", rows=len(result))
        return result

    def _fetch_assay_data(self, assay_ids: list[str]) -> pd.DataFrame:
        """Fetch assay data from ChEMBL API with release-scoped caching."""
        if not assay_ids:
            return pd.DataFrame()

        results: list[dict[str, Any]] = []
        pending: list[str] = []
        seen: set[str] = set()

        for assay_id in assay_ids:
            if not assay_id or assay_id in seen:
                continue
            seen.add(assay_id)
            cached = self._cache_get(assay_id)
            if cached is not None:
                results.append(cached)
            else:
                pending.append(assay_id)

        if not pending:
            logger.info("assay_fetch_served_from_cache", count=len(results))
            return pd.DataFrame(results)

        for index in range(0, len(pending), self.batch_size):
            batch_ids = pending[index : index + self.batch_size]
            logger.info(
                "fetching_batch",
                batch=index // self.batch_size + 1,
                size=len(batch_ids),
            )

            try:
                response = self.api_client.request_json(
                    "/assay.json",
                    params={"assay_chembl_id__in": ",".join(batch_ids)},
                )
            except CircuitBreakerOpenError as exc:
                logger.error("assay_fetch_circuit_open", error=str(exc), batch_ids=batch_ids)
                for assay_id in batch_ids:
                    fallback = self._create_fallback_record(assay_id, exc)
                    self._cache_set(assay_id, fallback)
                    results.append(fallback)
                continue
            except requests.exceptions.RequestException as exc:
                logger.error("assay_fetch_request_exception", error=str(exc), batch_ids=batch_ids)
                for assay_id in batch_ids:
                    fallback = self._create_fallback_record(assay_id, exc)
                    self._cache_set(assay_id, fallback)
                    results.append(fallback)
                continue
            except Exception as exc:  # noqa: BLE001 - capture for fallback creation
                logger.error("assay_fetch_failed", error=str(exc), batch_ids=batch_ids)
                for assay_id in batch_ids:
                    fallback = self._create_fallback_record(assay_id, exc)
                    self._cache_set(assay_id, fallback)
                    results.append(fallback)
                continue

            assays_payload = []
            if isinstance(response, dict):
                assays_payload = response.get("assays", []) or []
            if not isinstance(assays_payload, list):
                assays_payload = []

            assays_by_id: dict[str, dict[str, Any]] = {}
            for assay in assays_payload:
                assay_id = assay.get("assay_chembl_id")
                if assay_id:
                    assays_by_id[str(assay_id)] = assay

            for assay_id in batch_ids:
                payload = assays_by_id.get(assay_id)
                if payload:
                    normalized = self._normalize_assay_record(payload)
                    self._cache_set(assay_id, normalized)
                    results.append(normalized)
                else:
                    logger.warning("assay_missing_from_response", assay_id=assay_id)
                    fallback = self._create_fallback_record(assay_id)
                    self._cache_set(assay_id, fallback)
                    results.append(fallback)

        if not results:
            logger.warning("no_results_from_api")
            return pd.DataFrame()

        df = pd.DataFrame(results)
        logger.info("api_extraction_completed", rows=len(df))
        return df

    def _expand_assay_parameters_long(self, df: pd.DataFrame) -> pd.DataFrame:
        """Expand assay parameters JSON into long-format rows."""

        if "assay_parameters_json" not in df.columns:
            return pd.DataFrame(columns=["assay_chembl_id", "row_subtype", "row_index"])

        records: list[dict[str, object]] = []

        for _, row in df.iterrows():
            params_raw = row.get("assay_parameters_json")
            if not params_raw or (isinstance(params_raw, float) and pd.isna(params_raw)):
                continue

            try:
                params = json.loads(params_raw) if isinstance(params_raw, str) else params_raw
            except (TypeError, ValueError):
                logger.warning("assay_param_parse_failed", assay_chembl_id=row.get("assay_chembl_id"))
                continue

            if not isinstance(params, Iterable):
                continue

            index = 0
            for param in params:
                if not isinstance(param, dict):
                    continue

                record = {
                    "assay_chembl_id": row.get("assay_chembl_id"),
                    "row_subtype": "param",
                    "row_index": index,
                    "assay_param_type": param.get("type"),
                    "assay_param_relation": param.get("relation"),
                    "assay_param_value": param.get("value"),
                    "assay_param_units": param.get("units"),
                    "assay_param_text_value": param.get("text_value"),
                    "assay_param_standard_type": param.get("standard_type"),
                    "assay_param_standard_value": param.get("standard_value"),
                    "assay_param_standard_units": param.get("standard_units"),
                }
                records.append(record)
                index += 1

        if not records:
            return pd.DataFrame(columns=["assay_chembl_id", "row_subtype", "row_index"])

        return pd.DataFrame(records)

    def _expand_variant_sequences(self, df: pd.DataFrame) -> pd.DataFrame:
        """Expand variant sequences JSON into long-format rows."""

        if "variant_sequence_json" not in df.columns:
            return pd.DataFrame(columns=["assay_chembl_id", "row_subtype", "row_index"])

        records: list[dict[str, object]] = []

        for _, row in df.iterrows():
            variant_raw = row.get("variant_sequence_json")
            if not variant_raw or (isinstance(variant_raw, float) and pd.isna(variant_raw)):
                continue

            try:
                variants = json.loads(variant_raw) if isinstance(variant_raw, str) else variant_raw
            except (TypeError, ValueError):
                logger.warning("variant_parse_failed", assay_chembl_id=row.get("assay_chembl_id"))
                continue

            if not isinstance(variants, Iterable):
                continue

            index = 0
            for variant in variants:
                if not isinstance(variant, dict):
                    continue

                record = {
                    "assay_chembl_id": row.get("assay_chembl_id"),
                    "row_subtype": "variant",
                    "row_index": index,
                    "variant_id": variant.get("variant_id"),
                    "variant_base_accession": variant.get("base_accession"),
                    "variant_mutation": variant.get("mutation"),
                    "variant_sequence": variant.get("variant_seq"),
                    "variant_accession_reported": variant.get("accession_reported"),
                }
                records.append(record)
                index += 1

        if not records:
            return pd.DataFrame(columns=["assay_chembl_id", "row_subtype", "row_index"])

        return pd.DataFrame(records)

    def _expand_assay_classifications(self, df: pd.DataFrame) -> pd.DataFrame:
        """Expand assay classifications JSON into long-format rows."""

        if "assay_classifications" not in df.columns:
            return pd.DataFrame(columns=["assay_chembl_id", "row_subtype", "row_index"])

        records: list[dict[str, object]] = []

        for _, row in df.iterrows():
            class_raw = row.get("assay_classifications")
            if not class_raw or (isinstance(class_raw, float) and pd.isna(class_raw)):
                continue

            parsed: Iterable[dict[str, object]] | None = None
            if isinstance(class_raw, str):
                try:
                    parsed_json = json.loads(class_raw)
                    if isinstance(parsed_json, list):
                        parsed = parsed_json
                except (TypeError, ValueError):
                    logger.warning(
                        "assay_class_parse_failed",
                        assay_chembl_id=row.get("assay_chembl_id"),
                    )
            elif isinstance(class_raw, Iterable):
                parsed = class_raw  # type: ignore[assignment]

            if not parsed:
                continue

            index = 0
            for classification in parsed:
                if not isinstance(classification, dict):
                    continue

                record = {
                    "assay_chembl_id": row.get("assay_chembl_id"),
                    "row_subtype": "class",
                    "row_index": index,
                    "assay_class_id": classification.get("assay_class_id"),
                    "assay_class_bao_id": classification.get("bao_id"),
                    "assay_class_type": classification.get("class_type"),
                    "assay_class_l1": classification.get("l1"),
                    "assay_class_l2": classification.get("l2"),
                    "assay_class_l3": classification.get("l3"),
                    "assay_class_description": classification.get("description"),
                }
                records.append(record)
                index += 1

        if not records:
            return pd.DataFrame(columns=["assay_chembl_id", "row_subtype", "row_index"])

        return pd.DataFrame(records)

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
        """Transform assay data with explode functionality."""
        if df.empty:
            # Return empty DataFrame with all required columns from schema
            from bioetl.schemas.assay import AssaySchema
            return pd.DataFrame(columns=AssaySchema.get_column_order())

        from bioetl.normalizers import registry

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

        # Target enrichment
        target_ids_series = base_df.get("target_chembl_id")
        if target_ids_series is not None:
            target_id_list = [tid for tid in target_ids_series.dropna().tolist() if tid]
            if target_id_list:
                target_reference = self._fetch_target_reference_data(target_id_list)
                matched_targets: set[str] = set()
                missing_count = 0

                if not target_reference.empty:
                    allowed_columns = [
                        col for col in target_reference.columns if col in TARGET_ENRICHMENT_WHITELIST
                    ]
                    target_reference = target_reference[allowed_columns]
                    base_df = base_df.merge(
                        target_reference,
                        on="target_chembl_id",
                        how="left",
                        suffixes=("", "_ref"),
                    )
                    matched_targets = set(
                        target_reference["target_chembl_id"].dropna().astype(str).unique().tolist()
                    )
                    if "pref_name" in base_df.columns:
                        pref_loss_mask = base_df["target_chembl_id"].notna() & base_df["pref_name"].isna()
                        missing_count = int(pref_loss_mask.sum())
                else:
                    missing_count = len(target_id_list)

                missing_targets = set(map(str, target_id_list)) - matched_targets
                missing_count = max(missing_count, len(missing_targets))

                if missing_count:
                    logger.warning("target_enrichment_join_loss", missing_count=missing_count)

        # Explode nested structures into long format dataframes
        params_df = self._expand_assay_parameters_long(base_df)
        variants_df = self._expand_variant_sequences(base_df)
        classes_df = self._expand_assay_classifications(base_df)

        # Assay class enrichment applied to classification rows
        if not classes_df.empty and "assay_class_id" in classes_df.columns:
            class_ids = [cid for cid in classes_df["assay_class_id"].dropna().tolist() if cid is not None]
            if class_ids:
                class_reference = self._fetch_assay_class_reference_data(class_ids)
                matched_classes: set[str] = set()
                missing_count = 0

                if not class_reference.empty:
                    allowed_columns = [
                        col for col in class_reference.columns if col in ASSAY_CLASS_ENRICHMENT_WHITELIST.values()
                    ]
                    class_reference = class_reference[allowed_columns]
                    classes_df = classes_df.merge(
                        class_reference,
                        on="assay_class_id",
                        how="left",
                        suffixes=("", "_ref"),
                    )
                    matched_classes = set(
                        class_reference["assay_class_id"].dropna().astype(str).unique().tolist()
                    )
                    for column in ASSAY_CLASS_ENRICHMENT_WHITELIST.values():
                        if column == "assay_class_id":
                            continue
                        ref_column = f"{column}_ref"
                        if ref_column in classes_df.columns:
                            classes_df[column] = classes_df[column].fillna(classes_df[ref_column])
                            classes_df.drop(columns=[ref_column], inplace=True)

                    missing_count = int(
                        (classes_df["assay_class_id"].notna() & classes_df["assay_class_bao_id"].isna()).sum()
                    )
                else:
                    missing_count = len(class_ids)

                missing_classes = set(map(str, class_ids)) - matched_classes
                missing_count = max(missing_count, len(missing_classes))

                if missing_count:
                    logger.warning("assay_class_enrichment_join_loss", missing_count=missing_count)

        frames = [base_df]
        for expanded in (params_df, variants_df, classes_df):
            if not expanded.empty:
                frames.append(expanded)

        df = pd.concat(frames, ignore_index=True, sort=False)

        # Normalise nullable integer columns to Pandas' nullable Int64 dtype so
        # Pandera can coerce them into the expected dtype('int64') during
        # validation. Mixed object/float columns originating from API payloads
        # previously triggered schema validation errors because Pandera refuses
        # to coerce values like "501" or NaN into strict integers when the
        # series dtype is ``object``. Explicitly converting here keeps the data
        # model consistent across all row subtypes.
        nullable_int_columns = [
            "row_index",
            "assay_class_id",
            "component_count",
            "variant_id",
            "species_group_flag",
            "tax_id",
            "assay_tax_id",
            "confidence_score",
            "src_id",
        ]

        if "row_index" in df.columns:
            df["row_index"] = df["row_index"].fillna(0)
        else:
            df["row_index"] = 0

        _coerce_nullable_int_columns(df, nullable_int_columns)

        # Add pipeline metadata
        df["pipeline_version"] = "1.0.0"
        if "source_system" in df.columns:
            df["source_system"] = df["source_system"].fillna("chembl")
        else:
            df["source_system"] = "chembl"
        df["chembl_release"] = self.chembl_release
        df["extracted_at"] = pd.Timestamp.now(tz="UTC").isoformat()

        # Generate hash fields for data integrity
        from bioetl.core.hashing import generate_hash_business_key, generate_hash_row

        df["hash_business_key"] = df["assay_chembl_id"].apply(generate_hash_business_key)
        df["hash_row"] = df.apply(lambda row: generate_hash_row(row.to_dict()), axis=1)

        # Generate deterministic index
        df = df.sort_values(["assay_chembl_id", "row_subtype", "row_index"])  # Sort by primary key
        df["index"] = range(len(df))

        # Reorder columns according to schema and add missing columns with None
        from bioetl.schemas import AssaySchema

        expected_cols = AssaySchema.get_column_order()
        if expected_cols:
            # Add missing columns with None values
            for col in expected_cols:
                if col not in df.columns:
                    df[col] = None

            # Reorder to match schema column_order
            df = df[expected_cols]

        return df

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate assay data against schema and referential integrity."""
        self.validation_issues.clear()

        if df.empty:
            logger.info("validation_skipped_empty", rows=0)
            return df

        schema_columns = list(AssaySchema.to_schema().columns.keys())
        if schema_columns:
            for column in schema_columns:
                if column not in df.columns:
                    df[column] = pd.NA
            df = df[schema_columns]
        else:  # pragma: no cover - defensive fallback
            expected_cols = AssaySchema.get_column_order()
            if expected_cols:
                for column in expected_cols:
                    if column not in df.columns:
                        df[column] = pd.NA
                df = df[expected_cols]

        try:
            _coerce_nullable_int_columns(df, _NULLABLE_INT_COLUMNS)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("nullable_int_cast_failed", error=str(exc))

        try:
            validated_df = AssaySchema.validate(df, lazy=True)
        except SchemaErrors as exc:
            schema_issues = self._summarize_schema_errors(exc.failure_cases)
            for issue in schema_issues:
                self.record_validation_issue(issue)
                logger.error(
                    "schema_validation_error",
                    column=issue.get("column"),
                    check=issue.get("check"),
                    count=issue.get("count"),
                    severity=issue.get("severity"),
                )

            summary = "; ".join(
                f"{issue.get('column')}: {issue.get('check')} ({issue.get('count')} cases)"
                for issue in schema_issues
            )
            raise ValueError(f"Schema validation failed: {summary}") from exc
        except Exception as exc:  # pragma: no cover - defensive
            logger.error("schema_validation_unexpected_error", error=str(exc))
            raise

        output_order = AssaySchema.get_column_order()
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

    def _summarize_schema_errors(self, failure_cases: pd.DataFrame) -> list[dict[str, Any]]:
        """Convert Pandera failure cases to structured validation issues."""

        issues: list[dict[str, Any]] = []
        if failure_cases.empty:
            return issues

        for column, group in failure_cases.groupby("column", dropna=False):
            column_name = (
                str(column)
                if column is not None and not (isinstance(column, float) and pd.isna(column))
                else "<dataframe>"
            )
            checks = sorted({str(check) for check in group["check"].dropna().unique()})
            details = ", ".join(
                group["failure_case"].dropna().astype(str).unique().tolist()[:5]
            )
            issues.append(
                {
                    "issue_type": "schema",
                    "severity": "error",
                    "column": column_name,
                    "check": ", ".join(checks) if checks else "<unspecified>",
                    "count": int(group.shape[0]),
                    "details": details,
                }
            )

        return issues

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
            .dropna()
            .astype(str)
            .str.strip()
            .str.upper()
        )
        reference_ids = set(values.tolist())
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

        target_series = df["target_chembl_id"].astype("string").str.upper()
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

