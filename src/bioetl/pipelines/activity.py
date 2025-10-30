"""Activity Pipeline - ChEMBL activity data extraction."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from pandera.errors import SchemaErrors

from bioetl.config import PipelineConfig
from bioetl.core.api_client import CircuitBreakerOpenError
from bioetl.core.logger import UnifiedLogger
from bioetl.normalizers import registry
from bioetl.pipelines.base import PipelineBase
from bioetl.schemas import ActivitySchema
from bioetl.schemas.registry import schema_registry
from bioetl.utils.dataframe import resolve_schema_column_order
from bioetl.utils.dtypes import coerce_nullable_float, coerce_nullable_int, coerce_retry_after
from bioetl.utils.fallback import FallbackRecordBuilder, build_fallback_payload
from bioetl.utils.json import normalize_json_list
from bioetl.utils.output import finalize_output_dataset
from bioetl.utils.qc import register_fallback_statistics

logger = UnifiedLogger.get(__name__)

# Register schema
schema_registry.register("activity", "1.0.0", ActivitySchema)


INTEGER_COLUMNS: tuple[str, ...] = (
    "standard_flag",
    "potential_duplicate",
    "src_id",
    "target_tax_id",
)
INTEGER_COLUMNS_WITH_ID: tuple[str, ...] = ("activity_id",) + INTEGER_COLUMNS
FLOAT_COLUMNS: tuple[str, ...] = ("fallback_retry_after_sec",)
NON_NEGATIVE_CACHE_COLUMNS: tuple[str, ...] = ("published_value", "standard_value")

ACTIVITY_FALLBACK_BUSINESS_COLUMNS: tuple[str, ...] = (
    "activity_id",
    "molecule_chembl_id",
    "assay_chembl_id",
    "target_chembl_id",
    "document_chembl_id",
    "published_type",
    "published_relation",
    "published_value",
    "published_units",
    "standard_type",
    "standard_relation",
    "standard_value",
    "standard_units",
    "standard_flag",
    "lower_bound",
    "upper_bound",
    "is_censored",
    "pchembl_value",
    "activity_comment",
    "data_validity_comment",
    "bao_endpoint",
    "bao_format",
    "bao_label",
    "canonical_smiles",
    "target_organism",
    "target_tax_id",
    "activity_properties",
    "compound_key",
    "is_citation",
    "high_citation_rate",
    "exact_data_citation",
    "rounded_data_citation",
    "potential_duplicate",
    "uo_units",
    "qudt_units",
    "src_id",
    "action_type",
    "bei",
    "sei",
    "le",
    "lle",
    "chembl_release",
    "source_system",
    "fallback_reason",
    "fallback_error_type",
    "fallback_error_code",
    "fallback_error_message",
    "fallback_http_status",
    "fallback_retry_after_sec",
    "fallback_attempt",
    "fallback_timestamp",
    "extracted_at",
)


@lru_cache(maxsize=1)
def _get_activity_column_order() -> list[str]:
    """Return the canonical Activity schema column order with robust fallbacks."""

    columns = resolve_schema_column_order(ActivitySchema)
    if not columns:
        try:
            columns = ActivitySchema.get_column_order()
        except AttributeError:  # pragma: no cover - defensive safeguard
            columns = []
    return list(columns)
def _derive_compound_key(
    molecule_id: str | None,
    standard_type: str | None,
    target_id: str | None,
) -> str | None:
    """Build compound key when all components are available."""

    if molecule_id and standard_type and target_id:
        return "|".join([molecule_id, standard_type, target_id])
    return None


def _derive_is_citation(
    document_id: str | None,
    properties: Sequence[dict[str, Any]],
) -> bool:
    """Infer citation flag from document linkage or properties."""

    if document_id:
        return True

    for prop in properties:
        label = str(prop.get("name") or prop.get("type") or "").lower()
        if label.replace(" ", "_") == "is_citation":
            return registry.get("numeric").normalize_bool(prop.get("value"), default=False)
    return False


def _derive_exact_data_citation(
    comment: str | None,
    properties: Sequence[dict[str, Any]],
) -> bool:
    """Derive exact data citation flag from comments or properties."""

    text = (comment or "").lower()
    if "exact" in text and "citation" in text:
        return True

    for prop in properties:
        label = str(prop.get("name") or prop.get("type") or "").lower().replace(" ", "_")
        if label == "exact_data_citation":
            return registry.get("numeric").normalize_bool(prop.get("value"), default=False)
    return False


def _derive_rounded_data_citation(
    comment: str | None,
    properties: Sequence[dict[str, Any]],
) -> bool:
    """Derive rounded data citation flag."""

    text = (comment or "").lower()
    if "rounded" in text and "citation" in text:
        return True

    for prop in properties:
        label = str(prop.get("name") or prop.get("type") or "").lower().replace(" ", "_")
        if label == "rounded_data_citation":
            return registry.get("numeric").normalize_bool(prop.get("value"), default=False)
    return False


def _derive_high_citation_rate(properties: Sequence[dict[str, Any]]) -> bool:
    """Detect high citation rate from property payloads."""

    for prop in properties:
        label = str(prop.get("name") or prop.get("type") or "").lower()
        if "citation" not in label:
            continue
        numeric = None
        for candidate in ("value", "num_value", "property_value", "count"):
            if candidate in prop:
                numeric = registry.normalize("numeric", prop.get(candidate))
                if numeric is not None:
                    break
        if numeric is not None and numeric >= 50:
            return True
        if label.replace(" ", "_") == "high_citation_rate":
            return registry.get("numeric").normalize_bool(prop.get("value"), default=False)
    return False


def _derive_is_censored(relation: str | None) -> bool | None:
    """Infer censorship flag from relation symbol."""

    if relation is None:
        return None
    return relation != "="


# _coerce_nullable_int_columns и _coerce_nullable_float_columns заменены на
# coerce_nullable_int и coerce_nullable_float из bioetl.utils.dtypes


class ActivityPipeline(PipelineBase):
    """Pipeline for extracting ChEMBL activity data."""

    def __init__(self, config: PipelineConfig, run_id: str):
        super().__init__(config, run_id)
        self._last_validation_report: dict[str, Any] | None = None
        self._fallback_stats: dict[str, Any] = {}

        chembl_context = self._init_chembl_client(
            defaults={
                "enabled": True,
                "base_url": "https://www.ebi.ac.uk/chembl/api/data",
                "batch_size": 25,
            },
            batch_size_cap=25,
        )
        # ``_init_chembl_client`` materializes a ``UnifiedAPIClient`` via ``APIClientFactory``
        # so that HTTP profiles, retry policies, rate limiting, circuit breaker tuning and
        # fallback behaviour from ``PipelineConfig`` are fully honoured.
        self.api_client = chembl_context.client
        self.batch_size = chembl_context.batch_size

        # Status handshake for release metadata
        self._status_snapshot: dict[str, Any] | None = None
        self._chembl_release = self._get_chembl_release()
        self._fallback_builder = FallbackRecordBuilder(
            business_columns=ACTIVITY_FALLBACK_BUSINESS_COLUMNS,
            context={"chembl_release": self._chembl_release},
        )

    def extract(self, input_file: Path | None = None) -> pd.DataFrame:
        """Extract activity data from input file."""
        expected_cols = ActivitySchema.get_column_order() or []
        df, resolved_path = self.read_input_table(
            default_filename=Path("activity.csv"),
            expected_columns=expected_cols,
            input_file=input_file,
        )

        if not resolved_path.exists():
            return df

        # Map activity_chembl_id to activity_id if needed
        if 'activity_chembl_id' in df.columns:
            df = df.rename(columns={'activity_chembl_id': 'activity_id'})
            df['activity_id'] = pd.to_numeric(df['activity_id'], errors='coerce').astype('Int64')

        # Extract activity IDs for API call
        activity_ids = df['activity_id'].dropna().astype(int).unique().tolist()

        limit_value = self.get_runtime_limit()
        if limit_value is not None and len(activity_ids) > limit_value:
            logger.info(
                "applying_input_limit",
                limit=limit_value,
                original_ids=len(activity_ids),
            )
            activity_ids = activity_ids[:limit_value]

        if not activity_ids:
            logger.warning("no_activity_ids_found")
            expected_cols = ActivitySchema.get_column_order()
            return pd.DataFrame(columns=expected_cols if expected_cols else [])

        logger.info("fetching_from_chembl_api", count=len(activity_ids))
        extracted_df = self._extract_from_chembl(activity_ids)

        if extracted_df.empty:
            logger.warning("no_api_data_returned")
            expected_cols = ActivitySchema.get_column_order()
            return pd.DataFrame(columns=expected_cols if expected_cols else [])

        logger.info("extraction_completed", rows=len(extracted_df), columns=len(extracted_df.columns))
        return extracted_df

    def _extract_from_chembl(self, activity_ids: list[int]) -> pd.DataFrame:
        """Extract activity data using release-scoped batching with caching."""

        if not activity_ids:
            return pd.DataFrame()

        success_count = 0
        fallback_count = 0
        error_count = 0
        api_calls = 0
        cache_hits = 0

        results: list[dict[str, Any]] = []

        for i in range(0, len(activity_ids), self.batch_size):
            batch_ids = activity_ids[i : i + self.batch_size]
            batch_number = i // self.batch_size + 1
            logger.info("fetching_batch", batch=batch_number, size=len(batch_ids))

            cached_records = self._load_batch_from_cache(batch_ids)
            if cached_records is not None:
                logger.info("cache_batch_hit", batch=batch_number, size=len(batch_ids))
                cache_hits += len(batch_ids)
                results.extend(cached_records)
                success_count += sum(
                    1
                    for record in cached_records
                    if record.get("source_system") != "ChEMBL_FALLBACK"
                )
                fallback_count += sum(
                    1
                    for record in cached_records
                    if record.get("source_system") == "ChEMBL_FALLBACK"
                )
                continue

            try:
                batch_records, batch_metrics = self._fetch_batch(batch_ids)
                api_calls += 1
                success_count += batch_metrics["success"]
                fallback_count += batch_metrics["fallback"]
                error_count += batch_metrics["error"]
                results.extend(batch_records)
                self._store_batch_in_cache(batch_ids, batch_records)
            except CircuitBreakerOpenError as error:
                logger.warning("circuit_breaker_open", batch=batch_number, error=str(error))
                fallback_records = [
                    self._create_fallback_record(
                        activity_id=activity_id,
                        reason="circuit_breaker_open",
                        error=error,
                    )
                    for activity_id in batch_ids
                ]
                results.extend(fallback_records)
                fallback_count += len(fallback_records)
            except Exception as error:  # noqa: BLE001 - surfaced for metrics
                error_count += len(batch_ids)
                logger.error("batch_fetch_failed", error=str(error), batch_ids=batch_ids)
                fallback_records = [
                    self._create_fallback_record(
                        activity_id=activity_id,
                        reason="exception",
                        error=error,
                    )
                    for activity_id in batch_ids
                ]
                results.extend(fallback_records)
                fallback_count += len(fallback_records)

        if not results:
            logger.warning("no_results_from_api")
            return pd.DataFrame()

        # Ensure deterministic ordering by activity_id
        results_sorted = sorted(results, key=lambda row: (row.get("activity_id") or 0, row.get("source_system", "")))

        logger.info(
            "chembl_activity_metrics",
            total_activities=len(activity_ids),
            success_count=success_count,
            fallback_count=fallback_count,
            error_count=error_count,
            success_rate=
            ((success_count + fallback_count) / len(activity_ids))
            if activity_ids
            else 0.0,
            api_calls=api_calls,
            cache_hits=cache_hits,
        )

        df = pd.DataFrame(results_sorted)
        if not df.empty:
            expected_columns = _get_activity_column_order()
            if expected_columns:
                extra_columns = [column for column in df.columns if column not in expected_columns]

                for column in expected_columns:
                    if column not in df.columns:
                        if column in INTEGER_COLUMNS_WITH_ID:
                            df[column] = pd.Series(pd.NA, index=df.index, dtype="Int64")
                        else:
                            df[column] = pd.Series(pd.NA, index=df.index)

                ordered_columns = [column for column in expected_columns if column in df.columns]
                df = df[ordered_columns + extra_columns]

            coerce_nullable_int(df, INTEGER_COLUMNS_WITH_ID)
        logger.info("extraction_completed", rows=len(df), from_api=True)
        return df

    def _fetch_batch(self, batch_ids: Iterable[int]) -> tuple[list[dict[str, Any]], dict[str, int]]:
        """Fetch a batch of activities from the ChEMBL API."""

        ids_str = ",".join(map(str, batch_ids))
        url = "/activity.json"
        params = {"activity_id__in": ids_str}

        response = self.api_client.request_json(url, params=params)

        activities = response.get("activities", [])
        metrics = {"success": 0, "fallback": 0, "error": 0}

        records: dict[int, dict[str, Any]] = {}
        for activity in activities:
            activity_id = activity.get("activity_id")
            if activity_id is None:
                continue
            record = self._normalize_activity(activity)
            records[int(activity_id)] = record

        missing_ids = [activity_id for activity_id in batch_ids if activity_id not in records]
        if missing_ids:
            metrics["error"] += len(missing_ids)
            for missing_id in missing_ids:
                records[missing_id] = self._create_fallback_record(
                    activity_id=missing_id,
                    reason="not_in_response",
                )

        metrics["success"] = sum(
            1 for record in records.values() if record.get("source_system") != "ChEMBL_FALLBACK"
        )
        metrics["fallback"] = sum(
            1 for record in records.values() if record.get("source_system") == "ChEMBL_FALLBACK"
        )

        ordered_records = [records[activity_id] for activity_id in sorted(records)]
        return ordered_records, metrics

    def _normalize_activity(self, activity: dict[str, Any]) -> dict[str, Any]:
        """Normalize raw activity payload into a flat record."""

        activity_id = registry.get("numeric").normalize_int(activity.get("activity_id"))
        molecule_id = registry.normalize("chemistry.chembl_id", activity.get("molecule_chembl_id"))
        assay_id = registry.normalize("chemistry.chembl_id", activity.get("assay_chembl_id"))
        target_id = registry.normalize("chemistry.chembl_id", activity.get("target_chembl_id"))
        document_id = registry.normalize("chemistry.chembl_id", activity.get("document_chembl_id"))

        published_type = registry.normalize(
            "chemistry.string",
            activity.get("type") or activity.get("published_type"),
            uppercase=True,
        )
        published_relation = registry.normalize(
            "chemistry.relation",
            activity.get("relation") or activity.get("published_relation"),
            default="=",
        )
        published_value = registry.normalize(
            "chemistry.non_negative_float",
            activity.get("value") or activity.get("published_value"),
            column="published_value",
        )
        published_units = registry.normalize(
            "chemistry.units",
            activity.get("units") or activity.get("published_units"),
        )

        standard_type = registry.normalize(
            "chemistry.string",
            activity.get("standard_type"),
            uppercase=True,
        )
        standard_relation = registry.normalize(
            "chemistry.relation",
            activity.get("standard_relation"),
            default="=",
        )
        standard_value = registry.normalize(
            "chemistry.non_negative_float",
            activity.get("standard_value"),
            column="standard_value",
        )
        standard_units = registry.normalize(
            "chemistry.units",
            activity.get("standard_units"),
            default="nM",
        )
        standard_flag = registry.get("numeric").normalize_int(activity.get("standard_flag"))

        lower_bound = registry.normalize("numeric", activity.get("standard_lower_value") or activity.get("lower_value"))
        upper_bound = registry.normalize("numeric", activity.get("standard_upper_value") or activity.get("upper_value"))
        is_censored = _derive_is_censored(standard_relation)

        pchembl_value = registry.normalize("numeric", activity.get("pchembl_value"))
        activity_comment = registry.normalize("chemistry.string", activity.get("activity_comment"))
        data_validity_comment = registry.normalize("chemistry.string", activity.get("data_validity_comment"))

        bao_endpoint = registry.normalize("chemistry.bao_id", activity.get("bao_endpoint"))
        bao_format = registry.normalize("chemistry.bao_id", activity.get("bao_format"))
        bao_label = registry.normalize("chemistry.string", activity.get("bao_label"), max_length=128)

        canonical_smiles = registry.normalize("chemistry.string", activity.get("canonical_smiles"))
        target_organism = registry.normalize("chemistry.target_organism", activity.get("target_organism"))
        target_tax_id = registry.get("numeric").normalize_int(activity.get("target_tax_id"))

        potential_duplicate = registry.get("numeric").normalize_int(activity.get("potential_duplicate"))
        uo_units = registry.normalize("chemistry.string", activity.get("uo_units"), uppercase=True)
        qudt_units = registry.normalize("chemistry.string", activity.get("qudt_units"))
        src_id = registry.get("numeric").normalize_int(activity.get("src_id"))
        action_type = registry.normalize("chemistry.string", activity.get("action_type"))

        properties_str, properties = normalize_json_list(activity.get("activity_properties"))
        ligand_efficiency = activity.get("ligand_efficiency") or activity.get("ligand_eff")
        bei, sei, le, lle = registry.normalize("chemistry.ligand_efficiency", ligand_efficiency)

        compound_key = _derive_compound_key(molecule_id, standard_type, target_id)
        is_citation = _derive_is_citation(document_id, properties)
        exact_citation = _derive_exact_data_citation(data_validity_comment, properties)
        rounded_citation = _derive_rounded_data_citation(data_validity_comment, properties)
        high_citation_rate = _derive_high_citation_rate(properties)

        record: dict[str, Any] = {
            "activity_id": activity_id,
            "molecule_chembl_id": molecule_id,
            "assay_chembl_id": assay_id,
            "target_chembl_id": target_id,
            "document_chembl_id": document_id,
            "published_type": published_type,
            "published_relation": published_relation,
            "published_value": published_value,
            "published_units": published_units,
            "standard_type": standard_type,
            "standard_relation": standard_relation,
            "standard_value": standard_value,
            "standard_units": standard_units,
            "standard_flag": standard_flag,
            "lower_bound": lower_bound,
            "upper_bound": upper_bound,
            "is_censored": is_censored,
            "pchembl_value": pchembl_value,
            "activity_comment": activity_comment,
            "data_validity_comment": data_validity_comment,
            "bao_endpoint": bao_endpoint,
            "bao_format": bao_format,
            "bao_label": bao_label,
            "canonical_smiles": canonical_smiles,
            "target_organism": target_organism,
            "target_tax_id": target_tax_id,
            "activity_properties": properties_str,
            "compound_key": compound_key,
            "is_citation": is_citation,
            "high_citation_rate": high_citation_rate,
            "exact_data_citation": exact_citation,
            "rounded_data_citation": rounded_citation,
            "potential_duplicate": potential_duplicate,
            "uo_units": uo_units,
            "qudt_units": qudt_units,
            "src_id": src_id,
            "action_type": action_type,
            "bei": bei,
            "sei": sei,
            "le": le,
            "lle": lle,
            "chembl_release": self._chembl_release,
            "source_system": "chembl",
            "fallback_reason": None,
            "fallback_error_type": None,
            "fallback_error_code": None,
            "fallback_error_message": None,
            "fallback_http_status": None,
            "fallback_retry_after_sec": None,
            "fallback_attempt": None,
            "fallback_timestamp": None,
            "extracted_at": datetime.now(timezone.utc).isoformat(),
        }

        return record

    def _create_fallback_record(
        self,
        activity_id: int,
        reason: str,
        error: Exception | None = None,
    ) -> dict[str, Any]:
        """Create deterministic fallback record enriched with error metadata."""

        record = self._fallback_builder.record(
            overrides={
                "activity_id": activity_id,
                "published_relation": "=",
                "standard_relation": "=",
                "standard_units": "nM",
                "is_citation": False,
                "high_citation_rate": False,
                "exact_data_citation": False,
                "rounded_data_citation": False,
                "extracted_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        metadata = build_fallback_payload(
            entity="activity",
            reason=reason,
            error=error,
            source="ChEMBL_FALLBACK",
            context=self._fallback_builder.context,
        )
        record.update(metadata)

        return record

    def _cache_key(self, batch_ids: Iterable[int]) -> str:
        """Create deterministic cache key for a batch of IDs."""

        normalized = ",".join(map(str, sorted(batch_ids)))
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

    def _cache_path(self, batch_ids: Iterable[int]) -> Path:
        """Return the cache file path for a batch."""

        cache_dir = self._cache_base_dir()
        cache_dir.mkdir(parents=True, exist_ok=True)
        return cache_dir / f"{self._cache_key(batch_ids)}.json"

    def _cache_base_dir(self) -> Path:
        """Build the base directory for cached responses."""

        base_dir = Path(self.config.cache.directory)
        if not base_dir.is_absolute():
            if self.config.paths.cache_root:
                base_dir = Path(self.config.paths.cache_root) / base_dir
            else:
                base_dir = Path(base_dir)

        entity_dir = base_dir / self.config.pipeline.entity
        if self.config.cache.release_scoped:
            release = self._chembl_release or "unknown"
            return entity_dir / release
        return entity_dir

    def _load_batch_from_cache(self, batch_ids: Iterable[int]) -> list[dict[str, Any]] | None:
        """Load cached batch if available."""

        if not self.config.cache.enabled:
            return None

        cache_path = self._cache_path(batch_ids)
        if not cache_path.exists():
            return None

        try:
            with cache_path.open("r", encoding="utf-8") as handle:
                data = json.load(handle)
        except json.JSONDecodeError:
            logger.warning("cache_corrupted", path=str(cache_path))
            cache_path.unlink(missing_ok=True)
            return None

        if not isinstance(data, list):
            logger.warning("cache_payload_unexpected", path=str(cache_path))
            return None

        ordered_records: list[dict[str, Any]] = []
        for raw_record in data:
            if not isinstance(raw_record, dict):
                logger.warning("cache_record_invalid", path=str(cache_path))
                return None

            ordered_records.append(dict(raw_record))

        return self._sanitize_cached_records(ordered_records)

    def _sanitize_cached_records(
        self, records: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Ensure cached payloads respect non-negative numeric constraints."""

        if not records:
            return records

        sanitized_records: list[dict[str, Any]] = []
        for record in records:
            sanitized = dict(record)
            activity_id = sanitized.get("activity_id")

            for column in NON_NEGATIVE_CACHE_COLUMNS:
                if column in sanitized:
                    sanitized[column] = self._sanitize_cached_non_negative(
                        sanitized.get(column),
                        column=column,
                        activity_id=activity_id,
                    )

            sanitized_records.append(sanitized)

        return sanitized_records

    def _sanitize_cached_non_negative(
        self,
        value: Any,
        *,
        column: str,
        activity_id: Any,
    ) -> float | None:
        """Normalise cached numeric values and drop negatives with observability."""

        result = registry.normalize("numeric", value)
        if result is None:
            return None

        if result < 0:
            logger.warning(
                "cached_non_negative_sanitized",
                column=column,
                original_value=result,
                sanitized_value=None,
                activity_id=activity_id,
            )
            return None

        return result

    def _store_batch_in_cache(self, batch_ids: Iterable[int], records: list[dict[str, Any]]) -> None:
        """Persist batch records into the local cache."""

        if not self.config.cache.enabled:
            return

        cache_path = self._cache_path(batch_ids)
        cache_path.parent.mkdir(parents=True, exist_ok=True)

        serializable = [
            dict(record)
            for record in sorted(
                records,
                key=lambda row: (row.get("activity_id") or 0, row.get("source_system", "")),
            )
        ]
        with cache_path.open("w", encoding="utf-8") as handle:
            json.dump(serializable, handle, ensure_ascii=False)

    def _get_chembl_release(self) -> str | None:
        """Get ChEMBL database release version from the status endpoint."""

        release = self._fetch_chembl_release_info(self.api_client)
        status = release.status
        if isinstance(status, Mapping):
            self._status_snapshot = dict(status) if not isinstance(status, dict) else status
        else:
            self._status_snapshot = None

        return release.version

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform activity data."""
        if df.empty:
            return df

        df = df.copy()

        pipeline_version = getattr(self.config.pipeline, "version", None) or "1.0.0"
        default_source = "chembl"

        if "source_system" in df.columns:
            df["source_system"] = df["source_system"].fillna(default_source)
        else:
            df["source_system"] = default_source

        release_value: str | None = self._chembl_release
        if isinstance(release_value, str):
            release_value = release_value.strip() or None

        if release_value is None:
            if "chembl_release" in df.columns:
                df["chembl_release"] = df["chembl_release"].where(
                    df["chembl_release"].notna(),
                    pd.NA,
                )
            else:
                df["chembl_release"] = pd.Series(pd.NA, index=df.index, dtype="string")
        else:
            if "chembl_release" in df.columns:
                df["chembl_release"] = df["chembl_release"].fillna(release_value)
            else:
                df["chembl_release"] = release_value

        timestamp_now = datetime.now(timezone.utc).isoformat()
        if "extracted_at" in df.columns:
            df["extracted_at"] = df["extracted_at"].fillna(timestamp_now)
        else:
            df["extracted_at"] = timestamp_now

        coerce_nullable_int(df, INTEGER_COLUMNS_WITH_ID)

        df = finalize_output_dataset(
            df,
            business_key="activity_id",
            sort_by=["activity_id", "source_system"],
            ascending=[True, True],
            schema=ActivitySchema,
            metadata={
                "pipeline_version": pipeline_version,
                "run_id": self.run_id,
                "source_system": default_source,
                "chembl_release": release_value,
                "extracted_at": timestamp_now,
            },
        )

        self.set_export_metadata_from_dataframe(
            df,
            pipeline_version=pipeline_version,
            source_system=default_source,
            chembl_release=release_value,
        )

        self._update_fallback_artifacts(df)

        coerce_nullable_int(df, INTEGER_COLUMNS_WITH_ID)

        df = df.convert_dtypes()
        coerce_retry_after(df)

        coerce_nullable_float(df, FLOAT_COLUMNS)

        if "is_censored" in df.columns:
            df["is_censored"] = df["is_censored"].astype("boolean")

        for column in INTEGER_COLUMNS:
            if column in df.columns:
                df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")

        return df

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate activity data against schema."""
        self.validation_issues.clear()

        if df.empty:
            logger.info("validation_skipped_empty", rows=0)
            self._last_validation_report = {
                "metrics": {},
                "schema_validation": {"status": "skipped"},
                "issues": [],
            }
            return df

        df = df.copy()

        expected_columns = _get_activity_column_order()
        if expected_columns:
            missing_columns = [column for column in expected_columns if column not in df.columns]
            if missing_columns:
                for column in missing_columns:
                    if column in INTEGER_COLUMNS_WITH_ID:
                        df[column] = pd.Series(pd.NA, index=df.index, dtype="Int64")
                    elif column in FLOAT_COLUMNS:
                        df[column] = pd.Series(np.nan, index=df.index, dtype="float64")
                    else:
                        df[column] = pd.Series(pd.NA, index=df.index)
                logger.debug(
                    "validation_missing_columns_filled",
                    columns=missing_columns,
                )

            extra_columns = [column for column in df.columns if column not in expected_columns]
            ordered_columns = list(expected_columns) + extra_columns
            if list(df.columns) != ordered_columns:
                logger.debug(
                    "validation_reordered_columns",
                    expected=len(expected_columns),
                    extras=extra_columns,
                )
                df = df[ordered_columns]

        coerce_retry_after(df)
        coerce_nullable_int(df, INTEGER_COLUMNS_WITH_ID)
        coerce_nullable_float(df, FLOAT_COLUMNS)

        qc_metrics = self._calculate_qc_metrics(df)
        fallback_stats = getattr(self, "_fallback_stats", None) or {}

        if fallback_stats:
            thresholds = getattr(self.config.qc, "thresholds", {}) or {}

            raw_count_threshold = thresholds.get("fallback.count")
            try:
                count_threshold = int(raw_count_threshold) if raw_count_threshold is not None else int(
                    fallback_stats.get("total_rows", len(df))
                )
            except (TypeError, ValueError):
                count_threshold = int(fallback_stats.get("total_rows", len(df)))

            raw_rate_threshold = thresholds.get("fallback.rate")
            try:
                rate_threshold = float(raw_rate_threshold) if raw_rate_threshold is not None else 1.0
            except (TypeError, ValueError):
                rate_threshold = 1.0

            fallback_count = int(fallback_stats.get("fallback_count", 0))
            fallback_rate = float(fallback_stats.get("fallback_rate", 0.0))

            count_severity = "info"
            if fallback_count > count_threshold:
                count_severity = "error"
            elif fallback_count > 0:
                count_severity = "warning"

            rate_severity = "info"
            if fallback_rate > rate_threshold:
                rate_severity = "error"
            elif fallback_rate > 0:
                rate_severity = "warning"

            qc_metrics["fallback.count"] = {
                "value": fallback_count,
                "threshold": count_threshold,
                "passed": fallback_count <= count_threshold,
                "severity": count_severity,
                "details": {
                    "activity_ids": fallback_stats.get("activity_ids", []),
                    "reason_counts": fallback_stats.get("reason_counts", {}),
                },
            }

            qc_metrics["fallback.rate"] = {
                "value": fallback_rate,
                "threshold": rate_threshold,
                "passed": fallback_rate <= rate_threshold,
                "severity": rate_severity,
                "details": {
                    "fallback_count": fallback_count,
                    "total_rows": fallback_stats.get("total_rows", len(df)),
                },
            }

            self.qc_summary_data.setdefault("metrics", {}).update(
                {
                    "fallback.count": qc_metrics["fallback.count"],
                    "fallback.rate": qc_metrics["fallback.rate"],
                }
            )

        self.set_qc_metrics(qc_metrics)
        self._last_validation_report = {"metrics": qc_metrics}

        failing_metrics: dict[str, Any] = {}
        for metric_name, metric in qc_metrics.items():
            log_method = logger.error if not metric["passed"] else logger.info
            log_method(
                "qc_metric",
                metric=metric_name,
                value=metric["value"],
                threshold=metric["threshold"],
                severity=metric["severity"],
                details=metric.get("details"),
            )

            issue_payload: dict[str, Any] = {
                "metric": f"qc.{metric_name}",
                "issue_type": "qc_metric",
                "severity": metric.get("severity", "info"),
                "value": metric.get("value"),
                "threshold": metric.get("threshold"),
                "passed": metric.get("passed"),
            }
            if metric.get("details"):
                issue_payload["details"] = metric["details"]
            self.record_validation_issue(issue_payload)

            if (not metric["passed"]) and self._should_fail(metric.get("severity", "info")):
                failing_metrics[metric_name] = metric

        if failing_metrics:
            self._last_validation_report["failing_metrics"] = failing_metrics
            if self._last_validation_report is not None:
                self._last_validation_report["issues"] = list(self.validation_issues)
            logger.error("qc_threshold_exceeded", failing_metrics=failing_metrics)
            raise ValueError("QC thresholds exceeded for metrics: " + ", ".join(failing_metrics.keys()))

        severity_threshold = self.config.qc.severity_threshold
        failure_report: dict[str, Any] | None = None

        def _handle_schema_failure(exc: SchemaErrors, should_fail: bool) -> None:
            nonlocal failure_report

            failure_cases = getattr(exc, "failure_cases", None)
            error_count: int | None = None
            if failure_cases is not None and hasattr(failure_cases, "shape"):
                try:
                    error_count = int(failure_cases.shape[0])
                except (TypeError, ValueError):
                    error_count = None

            if isinstance(failure_cases, pd.DataFrame) and not failure_cases.empty:
                try:
                    grouped = failure_cases.groupby("column", dropna=False)
                    for column, group in grouped:
                        column_name = (
                            str(column)
                            if column is not None
                            and not (isinstance(column, float) and pd.isna(column))
                            else "<dataframe>"
                        )
                        issue_details: dict[str, Any] = {
                            "metric": "schema.validation",
                            "issue_type": "schema_validation",
                            "severity": "critical",
                            "column": column_name,
                            "check": ", ".join(
                                sorted({str(check) for check in group["check"].dropna().unique()})
                            )
                            or "<unspecified>",
                            "count": int(group.shape[0]),
                        }
                        failure_examples = (
                            group["failure_case"].dropna().astype(str).unique().tolist()[:5]
                        )
                        if failure_examples:
                            issue_details["examples"] = failure_examples
                        self.record_validation_issue(issue_details)
                except Exception:  # pragma: no cover - defensive grouping fallback
                    self.record_validation_issue(
                        {
                            "metric": "schema.validation",
                            "issue_type": "schema_validation",
                            "severity": "critical",
                            "details": "failed_to_group_failure_cases",
                        }
                    )

            failure_report = {
                "status": "failed",
                "errors": error_count,
                "failure_cases": failure_cases,
                "should_fail": should_fail,
            }

            if not should_fail:
                logger.warning(
                    "schema_validation_below_threshold",
                    severity_threshold=severity_threshold,
                )

        def _handle_schema_success(validated: pd.DataFrame) -> None:
            if self._last_validation_report is not None:
                self._last_validation_report["schema_validation"] = {
                    "status": "passed",
                    "errors": 0,
                }
                self._last_validation_report["issues"] = list(self.validation_issues)

        try:
            validated_df = self._validate_with_schema(
                df,
                ActivitySchema,
                dataset_name="activity",
                severity="critical",
                metric_name="schema.validation",
                failure_handler=_handle_schema_failure,
                success_handler=_handle_schema_success,
            )
        except SchemaErrors:
            if self._last_validation_report is not None and failure_report is not None:
                self._last_validation_report["schema_validation"] = {
                    "status": "failed",
                    "errors": failure_report.get("errors"),
                    "failure_cases": failure_report.get("failure_cases"),
                }
                self._last_validation_report["issues"] = list(self.validation_issues)
            raise

        if failure_report and not failure_report.get("should_fail", False):
            if self._last_validation_report is not None:
                self._last_validation_report["schema_validation"] = {
                    "status": "failed",
                    "errors": failure_report.get("errors"),
                    "failure_cases": failure_report.get("failure_cases"),
                }
                self._last_validation_report["issues"] = list(self.validation_issues)
            return validated_df

        logger.info("schema_validation_passed", rows=len(validated_df))
        self.refresh_validation_issue_summary()
        return validated_df

    def _update_fallback_artifacts(self, df: pd.DataFrame) -> None:
        """Capture fallback diagnostics for QC reporting and additional outputs."""

        fallback_columns = [
            "activity_id",
            "source_system",
            "fallback_reason",
            "fallback_error_type",
            "fallback_error_message",
            "fallback_http_status",
            "fallback_error_code",
            "fallback_retry_after_sec",
            "fallback_attempt",
            "fallback_timestamp",
            "chembl_release",
            "extracted_at",
        ]
        fallback_records = register_fallback_statistics(
            df,
            summary=self.qc_summary_data,
            id_column="activity_id",
            fallback_columns=fallback_columns,
        )

        self._fallback_stats = self.qc_summary_data.get("fallbacks", {})

        fallback_count = int(self._fallback_stats.get("fallback_count", 0))

        if fallback_count:
            logger.warning(
                "chembl_fallback_records_detected",
                count=fallback_count,
                activity_ids=self._fallback_stats.get("ids"),
                reasons=self._fallback_stats.get("reason_counts"),
            )
            self.add_additional_table(
                "activity_fallback_records",
                fallback_records,
                relative_path=Path("qc") / "activity_fallback_records.csv",
            )
        else:
            self.remove_additional_table("activity_fallback_records")

    @property
    def last_validation_report(self) -> dict[str, Any] | None:
        """Return the most recent validation report."""
        return self._last_validation_report

    def _calculate_qc_metrics(self, df: pd.DataFrame) -> dict[str, Any]:
        """Compute QC metrics for validation."""
        qc_config = self.config.qc
        thresholds = qc_config.thresholds or {}

        metrics: dict[str, Any] = {}

        duplicate_threshold = thresholds.get("duplicates")
        duplicate_config = getattr(qc_config, "duplicate_check", None)
        duplicate_field = "activity_id"
        if duplicate_config and isinstance(duplicate_config, dict):
            duplicate_field = duplicate_config.get("field", duplicate_field)
            if duplicate_threshold is None:
                duplicate_threshold = duplicate_config.get("threshold")
        if duplicate_threshold is None:
            duplicate_threshold = 0

        duplicate_count = 0
        duplicate_values: list[Any] = []
        if duplicate_field in df.columns:
            duplicate_mask = df[duplicate_field].duplicated(keep=False)
            duplicate_count = int(df[duplicate_field].duplicated().sum())
            duplicate_values = df.loc[duplicate_mask, duplicate_field].tolist()

        metrics["duplicates"] = {
            "value": duplicate_count,
            "threshold": duplicate_threshold,
            "passed": duplicate_count <= duplicate_threshold,
            "severity": "error" if duplicate_count > duplicate_threshold else "info",
            "details": {"field": duplicate_field, "duplicate_values": duplicate_values},
        }

        critical_columns = ["standard_value", "standard_type", "molecule_chembl_id"]
        null_rates: dict[str, float] = {}
        column_thresholds: dict[str, float] = {}

        def _coerce_threshold(value: Any | None, default: float) -> float:
            """Return a float threshold, preserving explicit zero values."""

            if value is None:
                return float(default)
            try:
                return float(value)
            except (TypeError, ValueError):
                return float(default)

        null_rate_threshold = _coerce_threshold(thresholds.get("null_rate_critical"), 1.0)
        null_threshold_default = _coerce_threshold(
            thresholds.get("activity.null_fraction"), null_rate_threshold
        )

        for column in critical_columns:
            column_present = column in df.columns
            if column_present and len(df) > 0:
                null_fraction = float(df[column].isna().mean())
                null_count = int(df[column].isna().sum())
            elif column_present:
                null_fraction = 0.0
                null_count = 0
            else:
                null_fraction = 1.0
                null_count = len(df)

            column_threshold = _coerce_threshold(
                thresholds.get(f"activity.null_fraction.{column}"), null_threshold_default
            )

            null_rates[column] = null_fraction
            column_thresholds[column] = column_threshold

            metrics[f"null_fraction.{column}"] = {
                "value": null_fraction,
                "threshold": column_threshold,
                "passed": null_fraction <= column_threshold,
                "severity": "error" if null_fraction > column_threshold else "info",
                "details": {
                    "column": column,
                    "column_present": column_present,
                    "null_count": null_count,
                },
            }

        max_null_rate = max(null_rates.values()) if null_rates else 0.0
        metrics["null_rate"] = {
            "value": max_null_rate,
            "threshold": null_rate_threshold,
            "passed": max_null_rate <= null_rate_threshold,
            "severity": "error" if max_null_rate > null_rate_threshold else "info",
            "details": {
                "column_null_rates": null_rates,
                "column_thresholds": column_thresholds,
            },
        }

        invalid_units_threshold = float(thresholds.get("invalid_units", 0))
        invalid_units_count = 0
        invalid_indices: list[int] = []
        invalid_fraction = 0.0
        if {"standard_value", "standard_units"}.issubset(df.columns) and len(df) > 0:
            mask = df["standard_value"].notna() & df["standard_units"].isna()
            invalid_units_count = int(mask.sum())
            invalid_indices = df.index[mask].tolist()
            invalid_fraction = invalid_units_count / len(df)

        metrics["invalid_units"] = {
            "value": invalid_units_count,
            "threshold": invalid_units_threshold,
            "passed": invalid_units_count <= invalid_units_threshold,
            "severity": "error" if invalid_units_count > invalid_units_threshold else "info",
            "details": {"invalid_row_indices": invalid_indices, "invalid_fraction": invalid_fraction},
        }

        return metrics

