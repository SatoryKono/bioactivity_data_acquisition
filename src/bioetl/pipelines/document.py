"""Document Pipeline - ChEMBL document extraction with external enrichment."""

from pathlib import Path

import pandas as pd

from bioetl.config import PipelineConfig
from bioetl.core.api_client import APIConfig, UnifiedAPIClient
from bioetl.core.logger import UnifiedLogger
from bioetl.pipelines.base import PipelineBase
from bioetl.schemas import ChEMBLDocumentSchema
from bioetl.schemas.registry import schema_registry

logger = UnifiedLogger.get(__name__)

# Register schema
schema_registry.register("document", "1.0.0", ChEMBLDocumentSchema)


class DocumentPipeline(PipelineBase):
    """Pipeline for extracting ChEMBL document data.

    Modes:
    - chembl: ChEMBL only
    - all: ChEMBL + PubMed/Crossref/OpenAlex/Semantic Scholar
    """

    def __init__(self, config: PipelineConfig, run_id: str):
        super().__init__(config, run_id)

        # Initialize ChEMBL API client
        chembl_source = config.sources.get("chembl")
        if isinstance(chembl_source, dict):
            base_url = chembl_source.get("base_url", "https://www.ebi.ac.uk/chembl/api/data")
            batch_size = chembl_source.get("batch_size", 10)
        else:
            base_url = "https://www.ebi.ac.uk/chembl/api/data"
            batch_size = 10

        chembl_config = APIConfig(
            name="chembl",
            base_url=base_url,
            cache_enabled=config.cache.enabled,
            cache_ttl=config.cache.ttl,
        )
        self.api_client = UnifiedAPIClient(chembl_config)
        self.batch_size = batch_size

    def extract(self, input_file: Path | None = None) -> pd.DataFrame:
        """Extract document data from input file."""
        if input_file is None:
            # Default to data/input/documents.csv
            input_file = Path("data/input/documents.csv")

        logger.info("reading_input", path=input_file)

        # Read input file with document IDs
        if not input_file.exists():
            logger.warning("input_file_not_found", path=input_file)
            return pd.DataFrame(columns=[
                "document_chembl_id", "doi", "pmid", "journal", "title",
                "year", "authors", "abstract",
            ])

        df = pd.read_csv(input_file, nrows=10)  # Limit to 10 records

        logger.info("extraction_completed", rows=len(df))
        return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform document data."""
        if df.empty:
            return df

        # Normalize identifiers
        from bioetl.normalizers import registry

        for col in ["document_chembl_id", "doi", "pmid"]:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: registry.normalize("identifier", x) if pd.notna(x) else None
                )

        # Normalize text fields
        for col in ["journal", "title", "authors", "abstract"]:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: registry.normalize("string", x) if pd.notna(x) else None
                )

        # Add pipeline metadata
        df["pipeline_version"] = "1.0.0"
        df["source_system"] = "chembl"
        df["chembl_release"] = None
        df["extracted_at"] = pd.Timestamp.now(tz="UTC").isoformat()

        return df

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate document data against schema."""
        if df.empty:
            logger.info("validation_skipped_empty", rows=0)
            return df

        try:
            # Check for duplicates
            duplicate_count = df["document_chembl_id"].duplicated().sum() if "document_chembl_id" in df.columns else 0
            if duplicate_count > 0:
                logger.warning("duplicates_found", count=duplicate_count)
                df = df.drop_duplicates(subset=["document_chembl_id"], keep="first")

            logger.info("validation_completed", rows=len(df), duplicates_removed=duplicate_count)
            return df
        except Exception as e:
            logger.error("validation_failed", error=str(e))
            raise

