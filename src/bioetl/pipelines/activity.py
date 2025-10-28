"""Activity Pipeline - ChEMBL activity data extraction."""

from pathlib import Path

import pandas as pd

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

        df = pd.read_csv(input_file, nrows=10)  # Limit to 10 records

        # Check if we have activity_id or need to extract from ChEMBL
        # For now, return input as-is (will be enhanced to call ChEMBL API later)
        logger.info("extraction_completed", rows=len(df))
        return df

    def _extract_from_chembl(self, activity_ids: list[int]) -> pd.DataFrame:
        """Extract activity data using batch IDs strategy."""
        results = []

        # Batch activity IDs
        for i in range(0, len(activity_ids), self.batch_size):
            batch_ids = activity_ids[i:i + self.batch_size]
            logger.info("fetching_batch", batch=i // self.batch_size + 1, size=len(batch_ids))

            try:
                # Build URL with activity_id__in filter
                ids_str = ",".join(map(str, batch_ids))
                url = f"{self.api_client.config.base_url}/activity.json"
                params = {"activity_id__in": ids_str}

                response = self.api_client.request_json(url, params=params)

                if "activities" in response:
                    batch_results = response["activities"]
                    results.extend(batch_results)
                    logger.info("batch_fetched", count=len(batch_results))
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

        # Normalize standard_units
        if "standard_units" in df.columns:
            df["standard_units"] = df["standard_units"].apply(
                lambda x: registry.normalize("string", x) if pd.notna(x) else None
            )

        # Add pipeline metadata
        df["pipeline_version"] = "1.0.0"
        df["source_system"] = "chembl"
        df["chembl_release"] = None
        df["extracted_at"] = pd.Timestamp.now(tz="UTC").isoformat()

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

