"""Data normalization for activity records."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


class ActivityNormalizer:
    """Normalizes activity data according to business rules."""

    def __init__(self, config: dict[str, Any] | None = None):
        """Initialize normalizer with configuration."""
        self.config = config or {}
        
        # Relation mapping rules
        self.relation_mapping = self.config.get('normalization', {}).get('relation_mapping', {
            "=": {
                "lower_bound": "standard_value",
                "upper_bound": "standard_value", 
                "is_censored": False
            },
            ">=": {
                "lower_bound": "standard_value",
                "upper_bound": None,
                "is_censored": True
            },
            ">": {
                "lower_bound": "standard_value",
                "upper_bound": None,
                "is_censored": True
            },
            "<=": {
                "lower_bound": None,
                "upper_bound": "standard_value",
                "is_censored": True
            },
            "<": {
                "lower_bound": None,
                "upper_bound": "standard_value",
                "is_censored": True
            }
        })

    def normalize_activities(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize activity data according to business rules."""
        logger.info(f"Normalizing {len(df)} activity records")
        
        # Create a copy to avoid modifying original
        normalized_df = df.copy()
        
        # Add computed fields
        normalized_df = self._add_interval_fields(normalized_df)
        normalized_df = self._add_foreign_keys(normalized_df)
        normalized_df = self._add_quality_flags(normalized_df)
        
        # Remove foreign key columns, quality flags, and retrieved_at from output
        key_columns_to_remove = ['assay_key', 'target_key', 'document_key', 'testitem_key']
        quality_columns_to_remove = ['quality_flag', 'quality_reason']
        metadata_columns_to_remove = ['retrieved_at']
        all_columns_to_remove = key_columns_to_remove + quality_columns_to_remove + metadata_columns_to_remove
        normalized_df = normalized_df.drop(columns=all_columns_to_remove, errors='ignore')
        
        logger.info(f"Normalization completed. Output: {len(normalized_df)} records")
        return normalized_df

    def _add_interval_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add interval representation fields for censored data."""
        logger.debug("Adding interval fields for censored data")
        
        # Initialize interval fields
        df['lower_bound'] = None
        df['upper_bound'] = None
        df['is_censored'] = False
        
        # Process each relation type
        for relation, mapping in self.relation_mapping.items():
            mask = df['standard_relation'] == relation
            
            if mask.any():
                logger.debug(f"Processing {mask.sum()} records with relation '{relation}'")
                
                # Set lower bound
                if mapping['lower_bound'] == 'standard_value':
                    df.loc[mask, 'lower_bound'] = df.loc[mask, 'standard_value']
                elif mapping['lower_bound'] is None:
                    df.loc[mask, 'lower_bound'] = None
                
                # Set upper bound
                if mapping['upper_bound'] == 'standard_value':
                    df.loc[mask, 'upper_bound'] = df.loc[mask, 'standard_value']
                elif mapping['upper_bound'] is None:
                    df.loc[mask, 'upper_bound'] = None
                
                # Set censoring flag
                df.loc[mask, 'is_censored'] = mapping['is_censored']
        
        # Handle unsupported relations
        unsupported_mask = ~df['standard_relation'].isin(self.relation_mapping.keys())
        if unsupported_mask.any():
            unsupported_relations = df.loc[unsupported_mask, 'standard_relation'].unique()
            logger.warning(f"Found unsupported relations: {unsupported_relations}")
            # Mark as censored with no bounds for unsupported relations
            df.loc[unsupported_mask, 'is_censored'] = True
            df.loc[unsupported_mask, 'lower_bound'] = None
            df.loc[unsupported_mask, 'upper_bound'] = None
        
        return df

    def _add_foreign_keys(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add foreign key fields for star schema links."""
        logger.debug("Adding foreign key fields")
        
        # Map ChEMBL IDs to foreign keys
        df['assay_key'] = df['assay_chembl_id']
        df['target_key'] = df['target_chembl_id']
        df['document_key'] = df['document_chembl_id']
        df['testitem_key'] = df['molecule_chembl_id']
        
        return df

    def _add_quality_flags(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add quality assessment flags."""
        logger.debug("Adding quality flags")
        
        # Initialize quality fields
        df['quality_flag'] = 'unknown'
        df['quality_reason'] = None
        
        # Check for missing critical fields
        missing_assay = df['assay_key'].isna()
        missing_testitem = df['testitem_key'].isna()
        missing_standard_value = df['standard_value'].isna()
        missing_standard_type = df['standard_type'].isna()
        
        # Flag records with missing critical data
        critical_missing = missing_assay | missing_testitem | missing_standard_value | missing_standard_type
        if critical_missing.any():
            df.loc[critical_missing, 'quality_flag'] = 'poor'
            reasons = []
            if missing_assay.any():
                reasons.append('missing_assay_key')
            if missing_testitem.any():
                reasons.append('missing_testitem_key')
            if missing_standard_value.any():
                reasons.append('missing_standard_value')
            if missing_standard_type.any():
                reasons.append('missing_standard_type')
            df.loc[critical_missing, 'quality_reason'] = ';'.join(reasons)
        
        # Check for data validity issues
        validity_issues = df['data_validity_comment'].notna()
        if validity_issues.any():
            df.loc[validity_issues, 'quality_flag'] = 'warning'
            df.loc[validity_issues, 'quality_reason'] = 'data_validity_comment'
        
        # Check for activity comment issues
        activity_issues = df['activity_comment'].isin(['inconclusive', 'undetermined', 'unevaluated'])
        if activity_issues.any():
            df.loc[activity_issues, 'quality_flag'] = 'warning'
            df.loc[activity_issues, 'quality_reason'] = 'problematic_activity_comment'
        
        # Mark good quality records
        good_quality = (
            ~critical_missing & 
            ~validity_issues & 
            ~activity_issues &
            df['standard_relation'].isin(self.relation_mapping.keys())
        )
        if good_quality.any():
            df.loc[good_quality, 'quality_flag'] = 'good'
            df.loc[good_quality, 'quality_reason'] = None
        
        return df

    def validate_interval_consistency(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate consistency of interval fields."""
        logger.debug("Validating interval consistency")
        
        # Check censored records have exactly one bound
        censored_mask = df['is_censored'] == True
        if censored_mask.any():
            censored_df = df[censored_mask]
            
            # Both bounds present (should not happen for censored)
            both_bounds = censored_df['lower_bound'].notna() & censored_df['upper_bound'].notna()
            if both_bounds.any():
                logger.warning(f"Found {both_bounds.sum()} censored records with both bounds")
                # Quality flags исключены из вывода
                # df.loc[censored_mask & both_bounds, 'quality_flag'] = 'warning'
                # df.loc[censored_mask & both_bounds, 'quality_reason'] = 'censored_with_both_bounds'
            
            # No bounds present (should not happen for censored)
            no_bounds = censored_df['lower_bound'].isna() & censored_df['upper_bound'].isna()
            if no_bounds.any():
                logger.warning(f"Found {no_bounds.sum()} censored records with no bounds")
                # Quality flags исключены из вывода
                # df.loc[censored_mask & no_bounds, 'quality_flag'] = 'warning'
                # df.loc[censored_mask & no_bounds, 'quality_reason'] = 'censored_with_no_bounds'
        
        # Check non-censored records have both bounds and they match
        non_censored_mask = df['is_censored'] == False
        if non_censored_mask.any():
            non_censored_df = df[non_censored_mask]
            
            # Missing bounds
            missing_lower = non_censored_df['lower_bound'].isna()
            missing_upper = non_censored_df['upper_bound'].isna()
            if missing_lower.any() or missing_upper.any():
                logger.warning("Found non-censored records with missing bounds")
                # Quality flags исключены из вывода
                # df.loc[non_censored_mask & (missing_lower | missing_upper), 'quality_flag'] = 'warning'
                # df.loc[non_censored_mask & (missing_lower | missing_upper), 'quality_reason'] = 'non_censored_missing_bounds'
            
            # Bounds don't match standard_value
            bounds_mismatch = (
                (non_censored_df['lower_bound'] != non_censored_df['standard_value']) |
                (non_censored_df['upper_bound'] != non_censored_df['standard_value'])
            )
            if bounds_mismatch.any():
                logger.warning(f"Found {bounds_mismatch.sum()} non-censored records with mismatched bounds")
                # Quality flags исключены из вывода
                # df.loc[non_censored_mask & bounds_mismatch, 'quality_flag'] = 'warning'
                # df.loc[non_censored_mask & bounds_mismatch, 'quality_reason'] = 'bounds_mismatch'
        
        return df
