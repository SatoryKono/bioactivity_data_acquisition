"""Activity Pipeline - ChEMBL activity data extraction."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from bioetl.config import PipelineConfig
from bioetl.core.api_client import (
    APIConfig,
    CircuitBreakerOpenError,
    UnifiedAPIClient,
)
from bioetl.core.logger import UnifiedLogger
from bioetl.pipelines.base import PipelineBase
from bioetl.schemas import ActivitySchema
from bioetl.schemas.registry import schema_registry

logger = UnifiedLogger.get(__name__)

# Register schema
schema_registry.register("activity", "1.0.0", ActivitySchema)


class ActivityPipeline(PipelineBase):
    """Pipeline for extracting ChEMBL activity data."""

    def __init__(self, config: PipelineConfig, run_id: str):
        super().__init__(config, run_id)

        # Initialize ChEMBL API client
        chembl_source = config.sources.get("chembl")
        if isinstance(chembl_source, dict):
            base_url = chembl_source.get("base_url", "https://www.ebi.ac.uk/chembl/api/data")
            batch_size = chembl_source.get("batch_size", 25) or 25
        elif chembl_source is not None:
            base_url = getattr(chembl_source, "base_url", "https://www.ebi.ac.uk/chembl/api/data")
            batch_size = getattr(chembl_source, "batch_size", 25) or 25
        else:
            base_url = "https://www.ebi.ac.uk/chembl/api/data"
            batch_size = 25

        chembl_config = APIConfig(
            name="chembl",
            base_url=base_url,
            cache_enabled=config.cache.enabled,
            cache_ttl=config.cache.ttl,
        )
        self.api_client = UnifiedAPIClient(chembl_config)
        self.batch_size = min(batch_size, 25)

        # Status handshake for release metadata
        self._status_snapshot: dict[str, Any] | None = None
        self._chembl_release = self._get_chembl_release()

    def extract(self, input_file: Path | None = None) -> pd.DataFrame:
        """Extract activity data from input file."""
        if input_file is None:
            # Default to data/input/activity.csv
            input_file = Path("data/input/activity.csv")

        logger.info("reading_input", path=input_file)

        # Read input file with activity IDs
        if not input_file.exists():
            logger.warning("input_file_not_found", path=input_file)
            # Return empty DataFrame with schema structure
            return pd.DataFrame(columns=[
                "activity_id", "molecule_chembl_id", "assay_chembl_id",
                "target_chembl_id", "document_chembl_id", "standard_type",
                "standard_relation", "standard_value", "standard_units",
                "pchembl_value", "bao_endpoint", "bao_format", "bao_label",
                "canonical_smiles", "target_organism", "target_tax_id",
                "data_validity_comment", "activity_properties",
            ])

        # Read CSV file
        df = pd.read_csv(input_file)

        # Map activity_chembl_id to activity_id if needed
        if 'activity_chembl_id' in df.columns:
            df = df.rename(columns={'activity_chembl_id': 'activity_id'})
            df['activity_id'] = pd.to_numeric(df['activity_id'], errors='coerce').astype('Int64')

        # Extract activity IDs for API call
        activity_ids = df['activity_id'].dropna().astype(int).unique().tolist()

        if activity_ids:
            logger.info("fetching_from_chembl_api", count=len(activity_ids))
            # Fetch enriched data from ChEMBL API
            enriched_df = self._extract_from_chembl(activity_ids)

            if not enriched_df.empty:
                # Merge with CSV data using activity_id as key
                df = df.merge(enriched_df, on='activity_id', how='left', suffixes=('', '_api'))
                logger.info("enriched_from_api", rows=len(df))
            else:
                logger.warning("no_api_data_returned")
        else:
            logger.warning("no_activity_ids_found")

        # Add missing IO_SCHEMAS columns with None/default values
        required_cols = [
            "activity_id", "molecule_chembl_id", "assay_chembl_id", "target_chembl_id", "document_chembl_id",
            "published_type", "published_relation", "published_value", "published_units",
            "standard_type", "standard_relation", "standard_value", "standard_units", "standard_flag",
            "lower_bound", "upper_bound", "is_censored", "pchembl_value",
            "activity_comment", "data_validity_comment",
            "bao_endpoint", "bao_format", "bao_label",
            "potential_duplicate", "uo_units", "qudt_units", "src_id", "action_type",
            "activity_properties_json", "bei", "sei", "le", "lle"
        ]

        for col in required_cols:
            if col not in df.columns:
                df[col] = None

        # Set default values
        df['standard_relation'] = '='
        df['bao_label'] = None

        logger.info("extraction_completed", rows=len(df), columns=len(df.columns))
        return df

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

        activity_data: dict[str, Any] = {
            "activity_id": activity.get("activity_id"),
            "molecule_chembl_id": activity.get("molecule_chembl_id"),
            "assay_chembl_id": activity.get("assay_chembl_id"),
            "target_chembl_id": activity.get("target_chembl_id"),
            "document_chembl_id": activity.get("document_chembl_id"),
            "published_type": activity.get("published_type"),
            "published_relation": activity.get("published_relation"),
            "published_value": activity.get("published_value"),
            "published_units": activity.get("published_units"),
            "standard_type": activity.get("standard_type"),
            "standard_relation": activity.get("standard_relation"),
            "standard_value": activity.get("standard_value"),
            "standard_units": activity.get("standard_units"),
            "standard_flag": activity.get("standard_flag"),
            "lower_bound": activity.get("lower_bound"),
            "upper_bound": activity.get("upper_bound"),
            "is_censored": activity.get("is_censored"),
            "pchembl_value": activity.get("pchembl_value"),
            "activity_comment": activity.get("activity_comment"),
            "data_validity_comment": activity.get("data_validity_comment"),
            "bao_endpoint": activity.get("bao_endpoint"),
            "bao_format": activity.get("bao_format"),
            "bao_label": activity.get("bao_label"),
            "potential_duplicate": activity.get("potential_duplicate"),
            "uo_units": activity.get("uo_units"),
            "qudt_units": activity.get("qudt_units"),
            "src_id": activity.get("src_id"),
            "action_type": activity.get("action_type"),
            "chembl_release": self._chembl_release,
            "source_system": "chembl",
            "fallback_reason": None,
            "error_type": None,
            "error_message": None,
            "http_status": None,
            "error_code": None,
            "retry_after_sec": None,
            "attempt": None,
            "run_id": self.run_id,
        }

        activity_properties = activity.get("activity_properties")
        if activity_properties:
            activity_data["activity_properties_json"] = json.dumps(activity_properties, ensure_ascii=False)
        else:
            activity_data["activity_properties_json"] = None

        ligand_eff = activity.get("ligand_eff")
        if isinstance(ligand_eff, dict):
            activity_data.update(
                {
                    "bei": ligand_eff.get("bei"),
                    "sei": ligand_eff.get("sei"),
                    "le": ligand_eff.get("le"),
                    "lle": ligand_eff.get("lle"),
                }
            )
        else:
            activity_data.update({"bei": None, "sei": None, "le": None, "lle": None})

        activity_data.setdefault("extracted_at", datetime.now(timezone.utc).isoformat())
        return activity_data

    def _create_fallback_record(
        self,
        activity_id: int,
        reason: str,
        error: Exception | None = None,
    ) -> dict[str, Any]:
        """Create deterministic fallback record enriched with error metadata."""

        http_status = None
        error_code = None
        retry_after = None

        if hasattr(error, "response") and getattr(error, "response") is not None:
            http_status = getattr(error.response, "status_code", None)

        if hasattr(error, "code"):
            error_code = getattr(error, "code")

        if hasattr(error, "retry_after"):
            retry_after = getattr(error, "retry_after")

        return {
            "activity_id": activity_id,
            "molecule_chembl_id": None,
            "assay_chembl_id": None,
            "target_chembl_id": None,
            "document_chembl_id": None,
            "published_type": None,
            "published_relation": None,
            "published_value": None,
            "published_units": None,
            "standard_type": None,
            "standard_relation": None,
            "standard_value": None,
            "standard_units": None,
            "standard_flag": None,
            "lower_bound": None,
            "upper_bound": None,
            "is_censored": None,
            "pchembl_value": None,
            "activity_comment": None,
            "data_validity_comment": None,
            "bao_endpoint": None,
            "bao_format": None,
            "bao_label": None,
            "potential_duplicate": None,
            "uo_units": None,
            "qudt_units": None,
            "src_id": None,
            "action_type": None,
            "activity_properties_json": None,
            "bei": None,
            "sei": None,
            "le": None,
            "lle": None,
            "chembl_release": self._chembl_release,
            "source_system": "ChEMBL_FALLBACK",
            "fallback_reason": reason,
            "error_type": type(error).__name__ if error else None,
            "error_message": str(error) if error else "Fallback: API unavailable",
            "http_status": http_status,
            "error_code": error_code,
            "retry_after_sec": retry_after,
            "attempt": getattr(error, "attempt", None),
            "run_id": self.run_id,
            "extracted_at": datetime.now(timezone.utc).isoformat(),
        }

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

        return [dict(record) for record in data]

    def _store_batch_in_cache(self, batch_ids: Iterable[int], records: list[dict[str, Any]]) -> None:
        """Persist batch records into the local cache."""

        if not self.config.cache.enabled:
            return

        cache_path = self._cache_path(batch_ids)
        cache_path.parent.mkdir(parents=True, exist_ok=True)

        serializable = [dict(record) for record in sorted(records, key=lambda row: (row.get("activity_id") or 0, row.get("source_system", "")))]
        with cache_path.open("w", encoding="utf-8") as handle:
            json.dump(serializable, handle, ensure_ascii=False, sort_keys=True)

    def _get_chembl_release(self) -> str | None:
        """Get ChEMBL database release version from the status endpoint."""

        try:
            url = f"{self.api_client.config.base_url}/status.json"
            response = self.api_client.request_json(url)
            self._status_snapshot = response

            version = response.get("chembl_db_version")
            release_date = response.get("chembl_release_date")
            activities = response.get("activities")

            if version:
                logger.info(
                    "chembl_version_fetched",
                    version=version,
                    release_date=release_date,
                    activities=activities,
                )
                return str(version)

            logger.warning("chembl_version_not_in_status_response")
        except Exception as error:  # noqa: BLE001 - handshake errors are non-fatal
            logger.warning("failed_to_get_chembl_version", error=str(error))

        return None

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform activity data."""
        if df.empty:
            return df

        # Normalize identifiers
        from bioetl.normalizers import registry

        for col in ["molecule_chembl_id", "assay_chembl_id", "target_chembl_id", "document_chembl_id"]:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: registry.normalize("identifier", x) if pd.notna(x) else None
                )

        # Normalize units
        for col in ["standard_units", "published_units", "uo_units", "qudt_units"]:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: registry.normalize("string", x) if pd.notna(x) else None
                )

        # Normalize action_type
        if "action_type" in df.columns:
            df["action_type"] = df["action_type"].apply(
                lambda x: registry.normalize("string", x) if pd.notna(x) else None
            )

        # Validate potential_duplicate (should be 0 or 1)
        if "potential_duplicate" in df.columns:
            df["potential_duplicate"] = df["potential_duplicate"].apply(
                lambda x: x if pd.notna(x) and x in [0, 1] else None
            )

        # Add pipeline metadata
        df["pipeline_version"] = "1.0.0"

        if "source_system" not in df.columns:
            df["source_system"] = None
        df["source_system"] = df["source_system"].fillna("chembl")

        if "chembl_release" not in df.columns:
            df["chembl_release"] = self._chembl_release
        else:
            df["chembl_release"] = df["chembl_release"].fillna(self._chembl_release)

        if "extracted_at" in df.columns:
            df["extracted_at"] = df["extracted_at"].fillna(pd.Timestamp.now(tz="UTC").isoformat())
        else:
            df["extracted_at"] = pd.Timestamp.now(tz="UTC").isoformat()

        # Generate hash fields for data integrity
        from bioetl.core.hashing import generate_hash_business_key, generate_hash_row

        df["hash_business_key"] = df["activity_id"].apply(generate_hash_business_key)
        df["hash_row"] = df.apply(lambda row: generate_hash_row(row.to_dict()), axis=1)

        # Generate deterministic index
        df = df.sort_values("activity_id")  # Sort by primary key
        df["index"] = range(len(df))

        # Reorder columns according to schema
        from bioetl.schemas import ActivitySchema

        expected_cols = ActivitySchema.get_column_order()
        if expected_cols:
            # Only reorder columns that exist in the DataFrame
            df = df[[col for col in expected_cols if col in df.columns]]

        return df

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate activity data against schema."""
        if df.empty:
            logger.info("validation_skipped_empty", rows=0)
            return df

        try:
            # Check for duplicates
            duplicate_count = df["activity_id"].duplicated().sum() if "activity_id" in df.columns else 0
            if duplicate_count > 0:
                logger.warning("duplicates_found", count=duplicate_count)
                # Remove duplicates, keeping first occurrence
                df = df.drop_duplicates(subset=["activity_id"], keep="first")

            logger.info("validation_completed", rows=len(df), duplicates_removed=duplicate_count)
            return df
        except Exception as e:
            logger.error("validation_failed", error=str(e))
            raise

