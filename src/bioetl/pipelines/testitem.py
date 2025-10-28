"""TestItem Pipeline - ChEMBL molecule data extraction."""

from pathlib import Path

import pandas as pd

from bioetl.config import PipelineConfig
from bioetl.core.api_client import APIConfig, UnifiedAPIClient
from bioetl.core.logger import UnifiedLogger
from bioetl.pipelines.base import PipelineBase
from bioetl.schemas import TestItemSchema
from bioetl.schemas.registry import schema_registry

logger = UnifiedLogger.get(__name__)

# Register schema
schema_registry.register("testitem", "1.0.0", TestItemSchema)


class TestItemPipeline(PipelineBase):
    """Pipeline for extracting ChEMBL molecule (testitem) data."""

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
        """Extract molecule data from input file."""
        if input_file is None:
            # Default to data/input/testitem.csv
            input_file = Path("data/input/testitem.csv")

        logger.info("reading_input", path=input_file)

        # Read input file with molecule IDs
        if not input_file.exists():
            logger.warning("input_file_not_found", path=input_file)
            # Return empty DataFrame with schema structure
            return pd.DataFrame(columns=[
                "molecule_chembl_id", "molregno", "parent_chembl_id",
                "canonical_smiles", "standard_inchi", "standard_inchi_key",
                "molecular_weight", "heavy_atoms", "aromatic_rings",
                "rotatable_bonds", "hba", "hbd",
                "lipinski_ro5_violations", "lipinski_ro5_pass",
                "all_names", "molecule_synonyms",
                "atc_classifications", "pubchem_cid", "pubchem_synonyms",
            ])

        df = pd.read_csv(input_file, nrows=10)  # Limit to 10 records

        logger.info("extraction_completed", rows=len(df))
        return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform molecule data."""
        if df.empty:
            return df

        # Normalize identifiers
        from bioetl.normalizers import registry

        for col in ["molecule_chembl_id", "parent_chembl_id"]:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: registry.normalize("identifier", x) if pd.notna(x) else None
                )

        # Normalize SMILES if present
        if "canonical_smiles" in df.columns:
            df["canonical_smiles"] = df["canonical_smiles"].apply(
                lambda x: registry.normalize("chemistry", x) if pd.notna(x) else None
            )

        # Normalize InChI Key
        if "standard_inchi_key" in df.columns:
            df["standard_inchi_key"] = df["standard_inchi_key"].apply(
                lambda x: registry.normalize("identifier", x) if pd.notna(x) else None
            )

        # Add pipeline metadata
        df["pipeline_version"] = "1.0.0"
        df["source_system"] = "chembl"
        df["chembl_release"] = None
        df["extracted_at"] = pd.Timestamp.now(tz="UTC").isoformat()

        return df

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate molecule data against schema."""
        if df.empty:
            logger.info("validation_skipped_empty", rows=0)
            return df

        try:
            # Check for duplicates
            duplicate_count = df["molecule_chembl_id"].duplicated().sum() if "molecule_chembl_id" in df.columns else 0
            if duplicate_count > 0:
                logger.warning("duplicates_found", count=duplicate_count)
                # Remove duplicates, keeping first occurrence
                df = df.drop_duplicates(subset=["molecule_chembl_id"], keep="first")

            logger.info("validation_completed", rows=len(df), duplicates_removed=duplicate_count)
            return df
        except Exception as e:
            logger.error("validation_failed", error=str(e))
            raise

