"""Assay Pipeline - ChEMBL assay data extraction."""

import json
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
        df = pd.read_csv(input_file)  # Read all records

        # Create output DataFrame with just assay_chembl_id for API lookup
        result = pd.DataFrame()
        result["assay_chembl_id"] = df["assay_chembl_id"] if "assay_chembl_id" in df.columns else df["assay_chembl_id"]

        logger.info("extraction_completed", rows=len(result))
        return result

    def _fetch_assay_data(self, assay_ids: list[str]) -> pd.DataFrame:
        """Fetch assay data from ChEMBL API."""
        results = []
        
        batch_size = 20
        for i in range(0, len(assay_ids), batch_size):
            batch_ids = assay_ids[i:i + batch_size]
            logger.info("fetching_batch", batch=i // batch_size + 1, size=len(batch_ids))
            
            try:
                url = f"{self.api_client.config.base_url}/assay.json"
                params = {"assay_chembl_id__in": ",".join(batch_ids)}
                
                response = self.api_client.request_json(url, params=params)
                assays = response.get("assays", [])
                
                for assay in assays:
                    # Extract and serialize classifications with proper empty list handling
                    classifications = assay.get("assay_classifications")
                    classifications_str = " | ".join(classifications) if classifications else None
                    
                    # Extract and serialize parameters with proper empty list handling
                    params = assay.get("assay_parameters")
                    params_json = json.dumps(params, ensure_ascii=False) if params is not None else None
                    
                    assay_data = {
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
                    
                    # Handle assay_class
                    assay_class = assay.get("assay_class")
                    if isinstance(assay_class, dict):
                        assay_data.update({
                            "assay_class_id": assay_class.get("assay_class_id"),
                            "assay_class_bao_id": assay_class.get("bao_id"),
                            "assay_class_type": assay_class.get("assay_class_type"),
                            "assay_class_l1": assay_class.get("class_level_1"),
                            "assay_class_l2": assay_class.get("class_level_2"),
                            "assay_class_l3": assay_class.get("class_level_3"),
                            "assay_class_description": assay_class.get("assay_class_description"),
                        })
                    
                    # Handle variant_sequence (take first one for now)
                    variant_sequences = assay.get("variant_sequence")
                    variant_sequence_json = None
                    
                    if variant_sequences:
                        if isinstance(variant_sequences, list) and len(variant_sequences) > 0:
                            variant = variant_sequences[0]
                            assay_data.update({
                                "variant_id": variant.get("variant_id"),
                                "variant_base_accession": variant.get("base_accession"),
                                "variant_mutation": variant.get("mutation"),
                                "variant_sequence": variant.get("variant_seq"),
                                "variant_accession_reported": variant.get("accession_reported"),
                            })
                            variant_sequence_json = json.dumps(variant_sequences, ensure_ascii=False)
                        elif isinstance(variant_sequences, dict):
                            # Handle single variant as dict
                            assay_data.update({
                                "variant_id": variant_sequences.get("variant_id"),
                                "variant_base_accession": variant_sequences.get("base_accession"),
                                "variant_mutation": variant_sequences.get("mutation"),
                                "variant_sequence": variant_sequences.get("variant_seq"),
                                "variant_accession_reported": variant_sequences.get("accession_reported"),
                            })
                            variant_sequence_json = json.dumps([variant_sequences], ensure_ascii=False)
                    
                    assay_data["variant_sequence_json"] = variant_sequence_json
                    
                    results.append(assay_data)
                
                logger.info("batch_fetched", count=len(assays))
                
            except Exception as e:
                logger.error("batch_fetch_failed", error=str(e), batch_ids=batch_ids)
                # Continue with next batch
        
        if not results:
            logger.warning("no_results_from_api")
            return pd.DataFrame()
        
        df = pd.DataFrame(results)
        logger.info("api_extraction_completed", rows=len(df))
        return df

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
        df["source_system"] = "chembl"
        df["chembl_release"] = None
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

