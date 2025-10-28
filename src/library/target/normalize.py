"""Target data normalization module."""

from __future__ import annotations

import hashlib
import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


class TargetNormalizer:
    """Normalizer for target data."""
    
    def __init__(self, config: dict[str, Any]):
        """Initialize target normalizer with configuration."""
        self.config = config
        self.pipeline_version = config.get("pipeline", {}).get("version", "2.0.0")
        
        # Load column mapping from config
        self.column_mapping = self._load_column_mapping()
        self.expected_columns = self._load_expected_columns()
    
    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize target data.
        
        Args:
            df: Raw target data DataFrame
            
        Returns:
            Normalized target data DataFrame
        """
        logger.info(f"Normalizing {len(df)} target records")
        
        # Create a copy to avoid modifying original
        normalized_df = df.copy()
        
        # Normalize ChEMBL fields
        normalized_df = self._normalize_chembl_fields(normalized_df)
        
        # Add system fields
        normalized_df = self._add_system_fields(normalized_df)
        
        # Map column names to schema format
        normalized_df = self._map_column_names(normalized_df)
        
        # Process JSON fields to create additional columns
        normalized_df = self._process_target_components(normalized_df)
        normalized_df = self._process_protein_classifications(normalized_df)
        
        # Fill missing columns from config
        normalized_df = self._fill_missing_columns(normalized_df)
        
        # Generate hashes
        normalized_df = self._generate_hashes(normalized_df)
        
        logger.info(f"Normalization completed. Output: {len(normalized_df)} records")
        return normalized_df
    
    def _normalize_chembl_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize ChEMBL-specific fields."""
        # Normalize target_chembl_id
        if "target_chembl_id" in df.columns:
            df["target_chembl_id"] = df["target_chembl_id"].astype(str).str.strip()
        
        # Normalize pref_name (preferred name)
        if "pref_name" in df.columns:
            df["pref_name"] = df["pref_name"].astype(str).str.strip()
            df["pref_name"] = df["pref_name"].replace("nan", "")
        
        # Normalize target_type
        if "target_type" in df.columns:
            df["target_type"] = df["target_type"].astype(str).str.strip()
            df["target_type"] = df["target_type"].replace("nan", "")
        
        # Normalize organism
        if "organism" in df.columns:
            df["organism"] = df["organism"].astype(str).str.strip()
            df["organism"] = df["organism"].replace("nan", "")
        
        # Normalize tax_id
        if "tax_id" in df.columns:
            df["tax_id"] = pd.to_numeric(df["tax_id"], errors="coerce")
        
        return df
    
    def _add_system_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add system metadata fields."""
        from datetime import datetime
        
        # Add pipeline version
        df["pipeline_version"] = self.pipeline_version
        
        # Add source system
        df["source_system"] = "target_pipeline"
        
        # Add extraction timestamp
        df["extracted_at"] = datetime.utcnow().isoformat() + "Z"
        
        return df
    
    def _generate_hashes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generate hash fields for data integrity."""
        # Generate hash_row for each record
        df["hash_row"] = df.apply(self._generate_row_hash, axis=1)
        
        # Generate hash_business_key for business logic
        df["hash_business_key"] = df.apply(self._generate_business_key_hash, axis=1)
        
        return df
    
    def _generate_row_hash(self, row: pd.Series) -> str:
        """Generate hash for entire row."""
        # Convert row to string representation
        row_str = "|".join([str(val) for val in row.values if pd.notna(val)])
        
        # Generate SHA256 hash
        return hashlib.sha256(row_str.encode("utf-8")).hexdigest()[:16]
    
    def _generate_business_key_hash(self, row: pd.Series) -> str:
        """Generate hash for business key fields."""
        # Use target_chembl_id as primary business key
        business_key = str(row.get("target_chembl_id", ""))
        
        # Generate SHA256 hash
        return hashlib.sha256(business_key.encode("utf-8")).hexdigest()[:16]
    
    def _load_column_mapping(self) -> dict[str, str]:
        """Load column mapping from config."""
        logger.info(f"Config type: {type(self.config)}")
        logger.info(f"Config keys: {list(self.config.keys()) if hasattr(self.config, 'keys') else 'No keys method'}")
        
        # Handle both dict and object config
        if hasattr(self.config, 'determinism'):
            logger.info("Using object config determinism")
            column_order = self.config.determinism.column_order if hasattr(self.config.determinism, 'column_order') else []
        else:
            logger.info("Using dict config determinism")
            column_order = self.config.get("determinism", {}).get("column_order", [])
        
        logger.info(f"Column order length: {len(column_order)}")
        logger.info(f"First 5 columns: {column_order[:5] if column_order else 'Empty'}")
        
        # Create mapping from simple names to prefixed names
        mapping = {}
        for column_name in column_order:
            if column_name.startswith("CHEMBL.TARGETS."):
                simple_name = column_name.replace("CHEMBL.TARGETS.", "")
                mapping[simple_name] = column_name
                logger.debug(f"Added mapping: {simple_name} -> {column_name}")
            elif column_name.startswith("CHEMBL.TARGET_COMPONENTS."):
                # These will be handled separately in target_components processing
                pass
            elif column_name.startswith("CHEMBL.PROTEIN_CLASSIFICATION."):
                # These will be handled separately in protein_classifications processing
                pass
            else:
                # Keep as is for non-prefixed columns
                mapping[column_name] = column_name
        
        logger.info(f"Loaded column mapping: {mapping}")
        return mapping
    
    def _load_expected_columns(self) -> list[str]:
        """Load expected columns from config."""
        # Handle both dict and object config
        if hasattr(self.config, 'determinism'):
            return self.config.determinism.column_order if hasattr(self.config.determinism, 'column_order') else []
        else:
            return self.config.get("determinism", {}).get("column_order", [])
    
    def _map_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Map column names to schema format."""
        logger.info("Mapping column names to schema format")
        
        # Create a copy to avoid modifying original
        mapped_df = df.copy()
        
        # Apply column mapping
        for simple_name, prefixed_name in self.column_mapping.items():
            if simple_name in mapped_df.columns and simple_name != prefixed_name:
                # Rename the column
                mapped_df = mapped_df.rename(columns={simple_name: prefixed_name})
                logger.debug(f"Mapped {simple_name} -> {prefixed_name}")
        
        logger.info(f"Column mapping completed. Columns: {list(mapped_df.columns)}")
        return mapped_df
    
    def _fill_missing_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Fill missing columns from config with None values."""
        logger.info("Filling missing columns from config")
        
        # Create a copy to avoid modifying original
        filled_df = df.copy()
        
        # Add missing columns with None values
        for column_name in self.expected_columns:
            if column_name not in filled_df.columns:
                filled_df[column_name] = None
                logger.debug(f"Added missing column: {column_name}")
        
        logger.info(f"Missing columns filled. Total columns: {len(filled_df.columns)}")
        return filled_df
    
    def _process_target_components(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process target_components JSON and create additional columns."""
        logger.info("Processing target_components JSON")
        
        # Create a copy to avoid modifying original
        processed_df = df.copy()
        
        if "CHEMBL.TARGETS.target_components" in processed_df.columns:
            # Process each row's target_components
            for idx, row in processed_df.iterrows():
                target_components_str = row.get("CHEMBL.TARGETS.target_components")
                if target_components_str and isinstance(target_components_str, str):
                    try:
                        import json
                        components = json.loads(target_components_str)
                        
                        if isinstance(components, list) and components:
                            # Extract first protein component data
                            first_component = components[0]
                            
                            # Map component fields to prefixed columns
                            component_mapping = {
                                "component_description": "component_description",
                                "component_id": "CHEMBL.TARGET_COMPONENTS.component_id",
                                "relationship": "CHEMBL.TARGET_COMPONENTS.relationship",
                                "component_synonyms_gene": "CHEMBL.TARGET_COMPONENTS.component_synonyms_gene",
                                "accession": "CHEMBL.TARGET_COMPONENTS.accession",
                                "component_synonyms": "CHEMBL.TARGET_COMPONENTS.component_synonyms",
                                "component_synonyms_ec_code": "CHEMBL.TARGET_COMPONENTS.component_synonyms_ec_code",
                                "xref_id": "CHEMBL.TARGET_COMPONENTS.xref_id"
                            }
                            
                            for field, column_name in component_mapping.items():
                                if field in first_component:
                                    processed_df.at[idx, column_name] = first_component[field]
                                    
                    except (json.JSONDecodeError, KeyError, IndexError) as e:
                        logger.warning(f"Failed to process target_components for row {idx}: {e}")
        
        logger.info("Target components processing completed")
        return processed_df
    
    def _process_protein_classifications(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process protein_classifications JSON and create additional columns."""
        logger.info("Processing protein_classifications JSON")
        
        # Create a copy to avoid modifying original
        processed_df = df.copy()
        
        if "protein_classifications" in processed_df.columns:
            # Process each row's protein_classifications
            for idx, row in processed_df.iterrows():
                protein_classifications_str = row.get("protein_classifications")
                if protein_classifications_str and isinstance(protein_classifications_str, str):
                    try:
                        import json
                        classifications = json.loads(protein_classifications_str)
                        
                        if isinstance(classifications, dict):
                            # Map classification fields to prefixed columns
                            if "pref_name" in classifications:
                                processed_df.at[idx, "CHEMBL.PROTEIN_CLASSIFICATION.pref_name"] = classifications["pref_name"]
                                    
                    except (json.JSONDecodeError, KeyError) as e:
                        logger.warning(f"Failed to process protein_classifications for row {idx}: {e}")
        
        logger.info("Protein classifications processing completed")
        return processed_df
