"""Target data validation module."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd
import pandera as pa

from library.schemas.target_schema import TargetRawSchema
from library.schemas.target_schema_normalized import (
    TargetNormalizedSchema as TargetSchemaNormalized,
)

logger = logging.getLogger(__name__)


class TargetValidator:
    """Validator for target data."""

    def __init__(self, config: dict[str, Any]):
        """Initialize target validator with configuration."""
        self.config = config
        self.raw_schema = TargetRawSchema
        self.normalized_schema = TargetSchemaNormalized

    def validate_raw_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate raw target input data.

        Args:
            df: Raw target data DataFrame

        Returns:
            Validated raw data DataFrame

        Raises:
            pa.errors.SchemaError: If validation fails
        """
        logger.info(f"Validating {len(df)} raw target records")

        # Check required columns
        required_columns = ["target_chembl_id"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        # Basic validation - check for empty target_chembl_id
        if df["target_chembl_id"].isna().any():
            empty_count = df["target_chembl_id"].isna().sum()
            logger.warning(f"Found {empty_count} records with empty target_chembl_id")

        # Remove rows with empty target_chembl_id
        df_clean = df.dropna(subset=["target_chembl_id"])
        if len(df_clean) < len(df):
            removed_count = len(df) - len(df_clean)
            logger.warning(f"Removed {removed_count} records with empty target_chembl_id")

        # Validate target_chembl_id format
        df_clean = self._validate_target_chembl_id_format(df_clean)

        logger.info(f"Raw target validation completed. Valid records: {len(df_clean)}")
        return df_clean

    def validate_normalized_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate normalized target data.

        Args:
            df: Normalized target data DataFrame

        Returns:
            Validated normalized data DataFrame

        Raises:
            pa.errors.SchemaError: If validation fails
        """
        logger.info(f"Validating {len(df)} normalized target records")

        try:
            # Use Pandera schema for validation
            schema = self.normalized_schema.get_schema()
            validated_df = schema.validate(df)
            logger.info("Normalized target data passed Pandera validation")
            return validated_df
        except pa.errors.SchemaError as e:
            logger.error(f"Normalized target data validation failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected validation error: {e}")
            raise

    def _validate_target_chembl_id_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate target_chembl_id format."""
        # Convert to string and strip whitespace
        df["target_chembl_id"] = df["target_chembl_id"].astype(str).str.strip()

        # Check format - should be CHEMBL followed by digits
        invalid_format = ~df["target_chembl_id"].str.match(r"^CHEMBL\d+$", na=False)

        if invalid_format.any():
            invalid_ids = df[invalid_format]["target_chembl_id"].unique()
            logger.warning(f"Found {len(invalid_ids)} invalid target_chembl_id formats: {invalid_ids[:5]}")

            # Remove invalid formats
            df_clean = df[~invalid_format]
            removed_count = len(df) - len(df_clean)
            logger.warning(f"Removed {removed_count} records with invalid target_chembl_id format")
            return df_clean

        return df

    def get_validation_summary(self, df: pd.DataFrame) -> dict[str, Any]:
        """Get validation summary for target data.

        Args:
            df: Target data DataFrame

        Returns:
            Validation summary dictionary
        """
        summary = {
            "total_records": len(df),
            "required_fields": {},
            "data_quality": {}
        }

        # Check required fields
        required_fields = ["target_chembl_id", "pref_name", "target_type"]
        for field in required_fields:
            if field in df.columns:
                non_null_count = df[field].notna().sum()
                summary["required_fields"][field] = {
                    "total": len(df),
                    "non_null": non_null_count,
                    "null_count": len(df) - non_null_count,
                    "completeness": non_null_count / len(df) if len(df) > 0 else 0
                }

        # Data quality metrics
        if "target_chembl_id" in df.columns:
            unique_targets = df["target_chembl_id"].nunique()
            summary["data_quality"]["unique_targets"] = unique_targets
            summary["data_quality"]["duplicates"] = len(df) - unique_targets

        if "target_type" in df.columns:
            target_types = df["target_type"].value_counts().to_dict()
            summary["data_quality"]["target_types"] = target_types

        return summary
