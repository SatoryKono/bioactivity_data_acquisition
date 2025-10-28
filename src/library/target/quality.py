"""Target data quality filtering module."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


class TargetQualityFilter:
    """Quality filter for target data."""
    
    def __init__(self, config: dict[str, Any]):
        """Initialize target quality filter with configuration."""
        self.config = config
        self.quality_threshold = config.get("quality", {}).get("threshold", 0.8)
    
    def filter_quality(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Filter target data by quality criteria.
        
        Args:
            df: Target data DataFrame
            
        Returns:
            Tuple of (accepted_data, rejected_data)
        """
        logger.info(f"Applying quality filters to {len(df)} target records")
        
        # Create quality flags
        df_with_flags = self._add_quality_flags(df)
        
        # Separate accepted and rejected data
        accepted_mask = df_with_flags["quality_flag"] == "accepted"
        accepted_data = df_with_flags[accepted_mask].copy()
        rejected_data = df_with_flags[~accepted_mask].copy()
        
        # Remove quality flag columns from output
        quality_columns = ["quality_flag", "quality_reason"]
        for col in quality_columns:
            if col in accepted_data.columns:
                accepted_data = accepted_data.drop(columns=[col])
            if col in rejected_data.columns:
                rejected_data = rejected_data.drop(columns=[col])
        
        logger.info(f"Quality filtering completed: {len(accepted_data)} accepted, {len(rejected_data)} rejected")
        return accepted_data, rejected_data
    
    def _add_quality_flags(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add quality flags to DataFrame."""
        df_with_flags = df.copy()
        
        # Initialize quality flags
        df_with_flags["quality_flag"] = "accepted"
        df_with_flags["quality_reason"] = ""
        
        # Apply quality checks
        df_with_flags = self._check_required_fields(df_with_flags)
        df_with_flags = self._check_data_completeness(df_with_flags)
        df_with_flags = self._check_data_consistency(df_with_flags)
        
        return df_with_flags
    
    def _check_required_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Check required fields are present and valid."""
        # Check target_chembl_id
        if "target_chembl_id" in df.columns:
            invalid_target_id = (
                df["target_chembl_id"].isna() |
                (df["target_chembl_id"].astype(str).str.strip() == "") |
                (~df["target_chembl_id"].astype(str).str.match(r"^CHEMBL\d+$", na=False))
            )
            
            if invalid_target_id.any():
                df.loc[invalid_target_id, "quality_flag"] = "rejected"
                df.loc[invalid_target_id, "quality_reason"] = "invalid_target_chembl_id"
                logger.warning(f"Rejected {invalid_target_id.sum()} records with invalid target_chembl_id")
        
        return df
    
    def _check_data_completeness(self, df: pd.DataFrame) -> pd.DataFrame:
        """Check data completeness."""
        # Check for completely empty records
        if "pref_name" in df.columns and "target_type" in df.columns:
            empty_records = (
                df["pref_name"].isna() &
                df["target_type"].isna()
            )
            
            if empty_records.any():
                df.loc[empty_records, "quality_flag"] = "rejected"
                df.loc[empty_records, "quality_reason"] = "incomplete_data"
                logger.warning(f"Rejected {empty_records.sum()} records with incomplete data")
        
        return df
    
    def _check_data_consistency(self, df: pd.DataFrame) -> pd.DataFrame:
        """Check data consistency."""
        # Check for duplicate target_chembl_id
        if "target_chembl_id" in df.columns:
            duplicates = df["target_chembl_id"].duplicated(keep="first")
            
            if duplicates.any():
                df.loc[duplicates, "quality_flag"] = "rejected"
                df.loc[duplicates, "quality_reason"] = "duplicate_target_chembl_id"
                logger.warning(f"Rejected {duplicates.sum()} duplicate target_chembl_id records")
        
        return df
    
    def get_quality_metrics(self, df: pd.DataFrame) -> dict[str, Any]:
        """Get quality metrics for target data.
        
        Args:
            df: Target data DataFrame
            
        Returns:
            Quality metrics dictionary
        """
        metrics = {
            "total_records": len(df),
            "quality_checks": {}
        }
        
        # Required fields check
        if "target_chembl_id" in df.columns:
            valid_target_ids = df["target_chembl_id"].notna() & df["target_chembl_id"].astype(str).str.match(r"^CHEMBL\d+$", na=False)
            metrics["quality_checks"]["valid_target_chembl_id"] = {
                "valid": valid_target_ids.sum(),
                "invalid": (~valid_target_ids).sum(),
                "completeness": valid_target_ids.sum() / len(df) if len(df) > 0 else 0
            }
        
        # Data completeness
        if "pref_name" in df.columns:
            has_pref_name = df["pref_name"].notna() & (df["pref_name"].astype(str).str.strip() != "")
            metrics["quality_checks"]["has_pref_name"] = {
                "present": has_pref_name.sum(),
                "missing": (~has_pref_name).sum(),
                "completeness": has_pref_name.sum() / len(df) if len(df) > 0 else 0
            }
        
        if "target_type" in df.columns:
            has_target_type = df["target_type"].notna() & (df["target_type"].astype(str).str.strip() != "")
            metrics["quality_checks"]["has_target_type"] = {
                "present": has_target_type.sum(),
                "missing": (~has_target_type).sum(),
                "completeness": has_target_type.sum() / len(df) if len(df) > 0 else 0
            }
        
        # Duplicates check
        if "target_chembl_id" in df.columns:
            duplicates = df["target_chembl_id"].duplicated()
            metrics["quality_checks"]["duplicates"] = {
                "unique": df["target_chembl_id"].nunique(),
                "duplicates": duplicates.sum(),
                "duplicate_rate": duplicates.sum() / len(df) if len(df) > 0 else 0
            }
        
        return metrics
