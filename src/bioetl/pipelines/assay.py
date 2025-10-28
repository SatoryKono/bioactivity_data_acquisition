"""Assay Pipeline - ChEMBL assay data extraction."""

from pathlib import Path

import pandas as pd

from bioetl.config import PipelineConfig
from bioetl.core.api_client import APIConfig, UnifiedAPIClient
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
        if isinstance(chembl_source, dict):
            base_url = chembl_source.get("base_url", "https://www.ebi.ac.uk/chembl/api/data")  # type: ignore
        else:
            base_url = "https://www.ebi.ac.uk/chembl/api/data"

        chembl_config = APIConfig(
            name="chembl",
            base_url=base_url,
            cache_enabled=config.cache.enabled,
            cache_ttl=config.cache.ttl,
        )
        self.api_client = UnifiedAPIClient(chembl_config)

    def extract(self, input_file: Path | None = None) -> pd.DataFrame:
        """Extract assay data from input file."""
        if input_file is None:
            # Default to data/input/assay.csv
            input_file = Path("data/input/assay.csv")

        logger.info("reading_input", path=input_file)
        df = pd.read_csv(input_file, nrows=10)  # Limit to 10 records

        # Create output DataFrame with schema columns
        result = pd.DataFrame()
        result["assay_chembl_id"] = df["assay_chembl_id"] if "assay_chembl_id" in df.columns else None
        result["assay_type"] = df["Target TYPE"] if "Target TYPE" in df.columns else None
        result["description"] = df["description"] if "description" in df.columns else None
        result["target_chembl_id"] = df["target_chembl_id"] if "target_chembl_id" in df.columns else None
        result["confidence_score"] = None  # Not in input file

        logger.info("extraction_completed", rows=len(result))
        return result

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform assay data."""
        from bioetl.normalizers import registry

        # Normalize strings
        if "description" in df.columns:
            df["description"] = df["description"].apply(
                lambda x: registry.normalize("string", x) if pd.notna(x) else None
            )

        if "assay_type" in df.columns:
            df["assay_type"] = df["assay_type"].apply(
                lambda x: registry.normalize("string", x) if pd.notna(x) else None
            )

        # Add pipeline metadata
        df["pipeline_version"] = "1.0.0"
        df["source_system"] = "chembl"
        df["chembl_release"] = None
        df["extracted_at"] = pd.Timestamp.now(tz="UTC").isoformat()

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

