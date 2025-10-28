"""Activity Pipeline - ChEMBL activity data extraction."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from pandera.errors import SchemaErrors

from bioetl.config import PipelineConfig
from bioetl.core.api_client import APIConfig, UnifiedAPIClient
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
        self._last_validation_report: dict[str, Any] | None = None

        # Initialize ChEMBL API client
        chembl_source = config.sources.get("chembl")
        if isinstance(chembl_source, dict):
            base_url = chembl_source.get("base_url", "https://www.ebi.ac.uk/chembl/api/data")
            batch_size = chembl_source.get("batch_size", 25)
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
        self.batch_size = batch_size

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
        """Extract activity data using batch IDs strategy."""
        import json
        results = []

        # Batch activity IDs
        for i in range(0, len(activity_ids), self.batch_size):
            batch_ids = activity_ids[i:i + self.batch_size]
            logger.info("fetching_batch", batch=i // self.batch_size + 1, size=len(batch_ids))

            try:
                # Build URL with activity_id__in filter
                ids_str = ",".join(map(str, batch_ids))
                url = "/activity.json"
                params = {"activity_id__in": ids_str}

                response = self.api_client.request_json(url, params=params)

                if "activities" in response:
                    activities = response["activities"]

                    # Process each activity and extract fields
                    for act in activities:
                        activity_data = {
                            "activity_id": act.get("activity_id"),
                            "molecule_chembl_id": act.get("molecule_chembl_id"),
                            "assay_chembl_id": act.get("assay_chembl_id"),
                            "target_chembl_id": act.get("target_chembl_id"),
                            "document_chembl_id": act.get("document_chembl_id"),
                            "published_type": act.get("published_type"),
                            "published_relation": act.get("published_relation"),
                            "published_value": act.get("published_value"),
                            "published_units": act.get("published_units"),
                            "standard_type": act.get("standard_type"),
                            "standard_relation": act.get("standard_relation"),
                            "standard_value": act.get("standard_value"),
                            "standard_units": act.get("standard_units"),
                            "standard_flag": act.get("standard_flag"),
                            "lower_bound": act.get("lower_bound"),
                            "upper_bound": act.get("upper_bound"),
                            "is_censored": act.get("is_censored"),
                            "pchembl_value": act.get("pchembl_value"),
                            "activity_comment": act.get("activity_comment"),
                            "data_validity_comment": act.get("data_validity_comment"),
                            "bao_endpoint": act.get("bao_endpoint"),
                            "bao_format": act.get("bao_format"),
                            "bao_label": act.get("bao_label"),
                            # New fields: ontologies and metadata
                            "potential_duplicate": act.get("potential_duplicate"),
                            "uo_units": act.get("uo_units"),
                            "qudt_units": act.get("qudt_units"),
                            "src_id": act.get("src_id"),
                            "action_type": act.get("action_type"),
                        }

                        # Handle activity_properties as JSON
                        activity_props = act.get("activity_properties")
                        if activity_props:
                            activity_data["activity_properties_json"] = json.dumps(activity_props, ensure_ascii=False)
                        else:
                            activity_data["activity_properties_json"] = None

                        # Handle ligand_eff
                        ligand_eff = act.get("ligand_eff")
                        if isinstance(ligand_eff, dict):
                            activity_data.update({
                                "bei": ligand_eff.get("bei"),
                                "sei": ligand_eff.get("sei"),
                                "le": ligand_eff.get("le"),
                                "lle": ligand_eff.get("lle"),
                            })
                        else:
                            activity_data.update({
                                "bei": None,
                                "sei": None,
                                "le": None,
                                "lle": None,
                            })

                        results.append(activity_data)

                    logger.info("batch_fetched", count=len(activities))
                else:
                    logger.warning("no_activities_in_response", response_keys=list(response.keys()))

            except Exception as e:
                logger.error("batch_fetch_failed", error=str(e), batch_ids=batch_ids)
                # Continue with next batch

        if not results:
            logger.warning("no_results_from_api")
            return pd.DataFrame()

        # Convert to DataFrame
        df = pd.DataFrame(results)
        logger.info("extraction_completed", rows=len(df), from_api=True)
        return df

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
        df["source_system"] = "chembl"
        df["chembl_release"] = None
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
        from bioetl.schemas.activity import COLUMN_ORDER as ACTIVITY_COLUMN_ORDER

        column_order = ACTIVITY_COLUMN_ORDER
        if column_order:
            # Only reorder columns that exist in the DataFrame
            df = df[[col for col in column_order if col in df.columns]]

        return df

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate activity data against schema."""
        if df.empty:
            logger.info("validation_skipped_empty", rows=0)
            return df

        qc_metrics = self._calculate_qc_metrics(df)
        self._last_validation_report = {"metrics": qc_metrics}

        severity_threshold = self.config.qc.severity_threshold.lower()
        severity_level_threshold = self._severity_level(severity_threshold)

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
            if (not metric["passed"]) and self._severity_level(metric["severity"]) >= severity_level_threshold:
                failing_metrics[metric_name] = metric

        if failing_metrics:
            self._last_validation_report["failing_metrics"] = failing_metrics
            logger.error("qc_threshold_exceeded", failing_metrics=failing_metrics)
            raise ValueError("QC thresholds exceeded for metrics: " + ", ".join(failing_metrics.keys()))

        try:
            validated_df = ActivitySchema.validate(df, lazy=True)
            self._last_validation_report["schema_validation"] = {
                "status": "passed",
                "errors": 0,
            }
            logger.info("schema_validation_passed", rows=len(validated_df))
            return validated_df
        except SchemaErrors as exc:
            failure_cases = exc.failure_cases if hasattr(exc, "failure_cases") else None
            if self._last_validation_report is not None:
                self._last_validation_report["schema_validation"] = {
                    "status": "failed",
                    "errors": len(failure_cases) if failure_cases is not None else None,
                    "failure_cases": failure_cases,
                }
            logger.error(
                "schema_validation_failed",
                error=str(exc),
                failure_count=len(failure_cases) if failure_cases is not None else None,
            )
            raise

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
        for column in critical_columns:
            if column in df.columns and len(df) > 0:
                null_rates[column] = float(df[column].isna().mean())
            else:
                null_rates[column] = 1.0 if column not in df.columns else 0.0

        null_rate_threshold = float(thresholds.get("null_rate_critical", 1.0))
        max_null_rate = max(null_rates.values()) if null_rates else 0.0
        metrics["null_rate"] = {
            "value": max_null_rate,
            "threshold": null_rate_threshold,
            "passed": max_null_rate <= null_rate_threshold,
            "severity": "error" if max_null_rate > null_rate_threshold else "info",
            "details": {"column_null_rates": null_rates},
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

    @staticmethod
    def _severity_level(severity: str) -> int:
        """Map severity string to comparable level."""
        levels = {"info": 0, "warning": 1, "error": 2, "critical": 3}
        return levels.get(severity.lower(), 1)

