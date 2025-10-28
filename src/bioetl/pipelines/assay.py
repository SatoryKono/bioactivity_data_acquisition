"""Assay Pipeline - ChEMBL assay data extraction."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from bioetl.config import PipelineConfig
from bioetl.core.api_client import APIConfig, CircuitBreakerOpenError, UnifiedAPIClient
from bioetl.core.logger import UnifiedLogger
from bioetl.pipelines.base import PipelineBase
from bioetl.schemas import AssaySchema
from bioetl.schemas.registry import schema_registry

logger = UnifiedLogger.get(__name__)

# Register schema
schema_registry.register("assay", "1.0.0", AssaySchema)


class AssayPipeline(PipelineBase):
    """Pipeline for extracting ChEMBL assay data."""

    def __init__(self, config: PipelineConfig, run_id: str):
        super().__init__(config, run_id)

        # Initialize ChEMBL API client
        chembl_source = config.sources.get("chembl")
        default_base = "https://www.ebi.ac.uk/chembl/api/data"
        if chembl_source is None:
            base_url = default_base
            batch_size = 25
        elif isinstance(chembl_source, dict):
            base_url = chembl_source.get("base_url", default_base)  # type: ignore[assignment]
            batch_size = int(chembl_source.get("batch_size", 25) or 25)
        else:
            base_url = getattr(chembl_source, "base_url", default_base)
            batch_size = int(getattr(chembl_source, "batch_size", 25) or 25)

        chembl_config = APIConfig(
            name="chembl",
            base_url=base_url,
            cache_enabled=config.cache.enabled,
            cache_ttl=config.cache.ttl,
        )
        self.api_client = UnifiedAPIClient(chembl_config)

        self.batch_size = batch_size
        self.chembl_base_url = base_url
        self.config_hash = config.config_hash
        self.git_commit = self._get_git_commit()
        self.chembl_release = self._fetch_chembl_release()
        self.run_metadata: dict[str, Any] = {
            "run_id": run_id,
            "git_commit": self.git_commit,
            "config_hash": self.config_hash,
            "chembl_release": self.chembl_release,
            "chembl_base_url": self.chembl_base_url,
        }

        # Persistent cache (release-scoped)
        self._cache_enabled = bool(config.cache.enabled)
        self._cache_namespace: Path | None = None
        if self._cache_enabled:
            cache_dir = Path(config.cache.directory)
            release_segment = str(self.chembl_release or "unknown") if config.cache.release_scoped else "global"
            self._cache_namespace = cache_dir / "assay" / release_segment
            self._cache_namespace.mkdir(parents=True, exist_ok=True)

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
        """Fetch assay data from ChEMBL API with caching and fallbacks."""
        results: list[dict[str, Any]] = []
        ids_to_fetch: list[str] = []

        for assay_id in assay_ids:
            cached = self._load_from_cache(assay_id)
            if cached is not None:
                logger.debug("cache_hit_release", assay_id=assay_id)
                results.append(cached)
            else:
                ids_to_fetch.append(assay_id)

        if not ids_to_fetch:
            if results:
                return pd.DataFrame(results)
            logger.warning("no_assays_to_fetch")
            return pd.DataFrame()

        for i in range(0, len(ids_to_fetch), self.batch_size):
            batch_ids = ids_to_fetch[i : i + self.batch_size]
            logger.info("fetching_batch", batch=i // self.batch_size + 1, size=len(batch_ids))

            try:
                url = f"{self.api_client.config.base_url}/assay.json"
                params = {"assay_chembl_id__in": ",".join(batch_ids)}
                response = self.api_client.request_json(url, params=params)
                assays = response.get("assays", []) if isinstance(response, dict) else []

                batch_records: dict[str, dict[str, Any]] = {}
                for assay in assays:
                    record = self._normalize_assay_record(assay)
                    assay_id = record.get("assay_chembl_id")
                    if not assay_id:
                        continue
                    batch_records[assay_id] = record
                    results.append(record)
                    self._store_in_cache(assay_id, record)

                missing_ids = [assay_id for assay_id in batch_ids if assay_id not in batch_records]
                for missing_id in missing_ids:
                    logger.warning("assay_missing_from_response", assay_id=missing_id)
                    results.append(self._create_fallback_record(missing_id))

                logger.info("batch_fetched", count=len(batch_records))

            except CircuitBreakerOpenError as exc:
                logger.error("circuit_breaker_open", error=str(exc))
                for assay_id in batch_ids:
                    results.append(self._create_fallback_record(assay_id, exc))
            except Exception as exc:  # noqa: BLE001 - broad for fallback creation
                logger.error("batch_fetch_failed", error=str(exc), batch_ids=batch_ids)
                for assay_id in batch_ids:
                    results.append(self._create_fallback_record(assay_id, exc))

        if not results:
            logger.warning("no_results_from_api")
            return pd.DataFrame()

        df = pd.DataFrame(results)
        logger.info("api_extraction_completed", rows=len(df))
        return df

    def _get_git_commit(self) -> str | None:
        """Return current git commit hash if available."""
        try:
            commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
            return commit or None
        except Exception as exc:  # noqa: BLE001 - best effort metadata
            logger.warning("git_commit_unavailable", error=str(exc))
            return None

    def _fetch_chembl_release(self) -> str | None:
        """Fetch ChEMBL release information from status endpoint."""
        try:
            url = f"{self.api_client.config.base_url}/status.json"
            response = self.api_client.request_json(url)
            if not isinstance(response, dict):
                logger.warning("chembl_status_unexpected_format")
                return None

            version = (
                response.get("chembl_db_version")
                or response.get("chembl_db_release")
                or response.get("chembl_release")
            )
            if version:
                logger.info(
                    "chembl_status_captured",
                    version=version,
                    base_url=self.api_client.config.base_url,
                )
                return str(version)

            logger.warning("chembl_release_missing_in_status")
            return None
        except Exception as exc:  # noqa: BLE001 - status is best effort
            logger.warning("chembl_status_failed", error=str(exc))
            return None

    def _cache_key(self, assay_id: str) -> str:
        """Compose cache key for assay records."""
        release = str(self.chembl_release or "unknown")
        return f"assay:{release}:{assay_id}"

    def _cache_path(self, key: str) -> Path | None:
        if not self._cache_namespace:
            return None
        safe_key = key.replace(":", "_")
        return self._cache_namespace / f"{safe_key}.json"

    def _load_from_cache(self, assay_id: str) -> dict[str, Any] | None:
        """Load assay data from persistent cache if available."""
        if not self._cache_enabled:
            return None

        key = self._cache_key(assay_id)
        path = self._cache_path(key)
        if not path or not path.exists():
            return None

        try:
            with path.open("r", encoding="utf-8") as handle:
                cached = json.load(handle)
                if isinstance(cached, dict):
                    return cached
                logger.warning("cache_entry_invalid", key=key)
        except Exception as exc:  # noqa: BLE001 - cache corruption shouldn't fail run
            logger.warning("cache_read_failed", key=key, error=str(exc))
        return None

    def _store_in_cache(self, assay_id: str, record: dict[str, Any]) -> None:
        """Persist assay record into cache."""
        if not self._cache_enabled:
            return

        key = self._cache_key(assay_id)
        path = self._cache_path(key)
        if not path:
            return

        try:
            with path.open("w", encoding="utf-8") as handle:
                json.dump(record, handle, ensure_ascii=False)
        except Exception as exc:  # noqa: BLE001 - cache writes shouldn't fail pipeline
            logger.warning("cache_write_failed", key=key, error=str(exc))

    def _normalize_assay_record(self, assay: dict[str, Any]) -> dict[str, Any]:
        """Normalize raw assay payload into flat record."""
        classifications = assay.get("assay_classifications")
        if isinstance(classifications, list):
            classifications_str = " | ".join([str(item) for item in classifications])
        else:
            classifications_str = None

        assay_params = assay.get("assay_parameters")
        params_json = json.dumps(assay_params, ensure_ascii=False) if assay_params is not None else None

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
            "source_system": "chembl",
            "error_code": None,
            "http_status": None,
            "error_message": None,
            "retry_after_sec": None,
            "attempt": None,
        }

        assay_class = assay.get("assay_class")
        if isinstance(assay_class, dict):
            record.update(
                {
                    "assay_class_id": assay_class.get("assay_class_id"),
                    "assay_class_bao_id": assay_class.get("bao_id"),
                    "assay_class_type": assay_class.get("assay_class_type"),
                    "assay_class_l1": assay_class.get("class_level_1"),
                    "assay_class_l2": assay_class.get("class_level_2"),
                    "assay_class_l3": assay_class.get("class_level_3"),
                    "assay_class_description": assay_class.get("assay_class_description"),
                }
            )

        variant_sequences = assay.get("variant_sequence")
        variant_sequence_json = None
        if variant_sequences:
            if isinstance(variant_sequences, list) and variant_sequences:
                variant = variant_sequences[0]
                if isinstance(variant, dict):
                    record.update(
                        {
                            "variant_id": variant.get("variant_id"),
                            "variant_base_accession": variant.get("base_accession"),
                            "variant_mutation": variant.get("mutation"),
                            "variant_sequence": variant.get("variant_seq"),
                            "variant_accession_reported": variant.get("accession_reported"),
                        }
                    )
                variant_sequence_json = json.dumps(variant_sequences, ensure_ascii=False)
            elif isinstance(variant_sequences, dict):
                record.update(
                    {
                        "variant_id": variant_sequences.get("variant_id"),
                        "variant_base_accession": variant_sequences.get("base_accession"),
                        "variant_mutation": variant_sequences.get("mutation"),
                        "variant_sequence": variant_sequences.get("variant_seq"),
                        "variant_accession_reported": variant_sequences.get("accession_reported"),
                    }
                )
                variant_sequence_json = json.dumps([variant_sequences], ensure_ascii=False)

        record["variant_sequence_json"] = variant_sequence_json
        return record

    def _create_fallback_record(self, assay_id: str, error: Exception | None = None) -> dict[str, Any]:
        """Create fallback record with error metadata."""
        error_code: Any = getattr(error, "code", None)
        http_status: Any = getattr(error, "status", None)
        retry_after: Any = getattr(error, "retry_after", None)
        attempt: Any = getattr(error, "attempt", None)
        message = "Fallback: API unavailable"

        if isinstance(error, requests.exceptions.HTTPError):
            response = error.response
            if response is not None:
                http_status = response.status_code
                error_code = getattr(response, "reason", None)
                retry_header = response.headers.get("Retry-After")
                if retry_header is not None:
                    try:
                        retry_after = float(retry_header)
                    except (TypeError, ValueError):
                        retry_after = None
            message = str(error)
        elif error is not None:
            message = str(error)

        return {
            "assay_chembl_id": assay_id,
            "source_system": "ChEMBL_FALLBACK",
            "error_code": error_code,
            "http_status": http_status,
            "error_message": message,
            "retry_after_sec": retry_after,
            "attempt": attempt,
        }

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform assay data with explode functionality."""
        if df.empty:
            # Return empty DataFrame with all required columns from schema
            return pd.DataFrame(columns=AssaySchema.Config.column_order)

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

        # Explode nested structures into long format
        # For now, create basic exploded rows (assay records)
        exploded_rows = []

        for _, row in df.iterrows():
            # Create base assay row
            assay_row = row.to_dict()
            assay_row["row_subtype"] = "assay"
            exploded_rows.append(assay_row)

            # TODO: In future, add explode for params and variants
            # if "assay_parameters" in row and pd.notna(row["assay_parameters"]):
            #     for idx, param in enumerate(row["assay_parameters"]):
            #         param_row = row.to_dict()
            #         param_row["row_subtype"] = "param"
            #         param_row["row_index"] = idx
            #         exploded_rows.append(param_row)

        df = pd.DataFrame(exploded_rows)

        # Generate row_index for deterministic ordering within each (assay_chembl_id, row_subtype) group
        df["row_index"] = df.groupby(["assay_chembl_id", "row_subtype"]).cumcount()

        # Add pipeline metadata
        df["pipeline_version"] = "1.0.0"
        if "source_system" not in df.columns:
            df["source_system"] = "chembl"
        else:
            df["source_system"] = df["source_system"].fillna("chembl")
        df["chembl_release"] = self.chembl_release
        df["extracted_at"] = pd.Timestamp.now(tz="UTC").isoformat()

        for col in ["error_code", "http_status", "error_message", "retry_after_sec", "attempt"]:
            if col not in df.columns:
                df[col] = None

        # Generate hash fields for data integrity
        from bioetl.core.hashing import generate_hash_business_key, generate_hash_row

        df["hash_business_key"] = df["assay_chembl_id"].apply(generate_hash_business_key)
        df["hash_row"] = df.apply(lambda row: generate_hash_row(row.to_dict()), axis=1)

        # Generate deterministic index
        df = df.sort_values(["assay_chembl_id", "row_subtype", "row_index"])  # Sort by primary key
        df["index"] = range(len(df))

        # Reorder columns according to schema and add missing columns with None
        from bioetl.schemas import AssaySchema

        if "column_order" in AssaySchema.Config.__dict__:
            expected_cols = AssaySchema.Config.column_order
            
            # Add missing columns with None values
            for col in expected_cols:
                if col not in df.columns:
                    df[col] = None
            
            # Reorder to match schema column_order
            df = df[expected_cols]

        return df

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate assay data against schema."""
        try:
            # Add confidence_score as int if missing
            if "confidence_score" in df.columns and df["confidence_score"].isna().all():
                df["confidence_score"] = df["confidence_score"].astype("Int64")  # Nullable int

            # Skip validation for now - will add later
            logger.info("validation_skipped", rows=len(df))
            return df
        except Exception as e:
            logger.error("validation_failed", error=str(e))
            raise

