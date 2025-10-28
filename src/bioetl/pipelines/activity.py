"""Activity Pipeline - ChEMBL activity data extraction."""

from pathlib import Path

import pandas as pd

from bioetl.config import PipelineConfig
from bioetl.core.api_client import APIConfig, UnifiedAPIClient
from bioetl.core.logger import UnifiedLogger
from bioetl.pipelines.activity_mappers import map_activity_row
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
            return pd.DataFrame(columns=ActivitySchema.Config.column_order)

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
                            "canonical_smiles": act.get("canonical_smiles"),
                            "target_organism": act.get("target_organism"),
                            "target_tax_id": act.get("target_tax_id"),
                            "activity_properties": act.get("activity_properties"),
                            "ligand_efficiency": act.get("ligand_efficiency") or act.get("ligand_eff"),
                            "potential_duplicate": act.get("potential_duplicate"),
                            "uo_units": act.get("uo_units"),
                            "qudt_units": act.get("qudt_units"),
                            "src_id": act.get("src_id"),
                            "action_type": act.get("action_type"),
                        }

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

        # Map raw rows to canonical schema fields
        records = [map_activity_row(row) for row in df.to_dict(orient="records")]
        normalized_df = pd.DataFrame(records)

        # Ensure all expected columns exist
        for column in ActivitySchema.Config.column_order:
            if column not in normalized_df.columns:
                if column in ActivitySchema.Config.string_columns:
                    normalized_df[column] = ""
                elif column in ActivitySchema.Config.bool_columns:
                    normalized_df[column] = False
                else:
                    normalized_df[column] = pd.NA

        # Apply NA policy for string columns
        for column in ActivitySchema.Config.string_columns:
            if column in normalized_df.columns:
                normalized_df[column] = normalized_df[column].fillna("")

        # Apply NA policy for boolean columns
        for column in ActivitySchema.Config.bool_columns:
            if column in normalized_df.columns:
                normalized_df[column] = normalized_df[column].fillna(False).astype(bool)

        int_columns = [
            "activity_id",
            "standard_flag",
            "potential_duplicate",
            "src_id",
            "target_tax_id",
        ]

        for column in int_columns:
            if column in normalized_df.columns:
                normalized_df[column] = pd.to_numeric(normalized_df[column], errors="coerce").astype("Int64")

        # Ensure numeric precision by rounding floats to 6 decimal places
        float_columns = [
            "published_value",
            "standard_value",
            "lower_bound",
            "upper_bound",
            "pchembl_value",
            "bei",
            "sei",
            "le",
            "lle",
        ]

        for column in float_columns:
            if column in normalized_df.columns:
                normalized_df[column] = pd.to_numeric(normalized_df[column], errors="coerce")
                normalized_df[column] = normalized_df[column].round(6)

        # Add pipeline metadata
        normalized_df["pipeline_version"] = "1.0.0"
        normalized_df["source_system"] = "chembl"
        normalized_df["chembl_release"] = ""
        normalized_df["extracted_at"] = pd.Timestamp.now(tz="UTC").isoformat()

        from bioetl.core.hashing import generate_hash_business_key, generate_hash_row

        normalized_df["hash_business_key"] = normalized_df["activity_id"].apply(generate_hash_business_key)
        normalized_df["hash_row"] = normalized_df.apply(lambda row: generate_hash_row(row.to_dict()), axis=1)

        normalized_df = normalized_df.sort_values("activity_id")
        normalized_df["index"] = range(len(normalized_df))

        expected_cols = ActivitySchema.Config.column_order
        normalized_df = normalized_df[expected_cols]

        return normalized_df

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

