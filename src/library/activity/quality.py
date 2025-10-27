"""Quality filtering and profiling for activity data."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


class ActivityQualityFilter:
    """Applies quality filters to activity data."""

    def __init__(self, config: dict[str, Any] | None = None):
        """Initialize quality filter with configuration."""
        self.config = config or {}

        # Get quality profile settings
        self.quality_profiles = self.config.get("quality_profiles", {})
        self.strict_config = self.quality_profiles.get("strict", {})
        self.moderate_config = self.quality_profiles.get("moderate", {})

        # Get normalization settings
        normalization_config = self.config.get("normalization", {})
        self.strict_activity_types = normalization_config.get("strict_activity_types", ["IC50", "Ki"])
        self.rejected_activity_comments = normalization_config.get("rejected_activity_comments", ["inconclusive", "undetermined", "unevaluated"])

    def apply_strict_quality_filter(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Apply strict quality filter and return (accepted, rejected) dataframes."""
        logger.info(f"Applying strict quality filter to {len(df)} records")

        if not self.strict_config.get("enabled", True):
            logger.info("Strict quality filter is disabled")
            return df, pd.DataFrame()

        # Start with all records
        accepted_mask = pd.Series([True] * len(df), index=df.index)
        rejection_reasons = []

        # Check required fields
        required_fields = self.strict_config.get("required_fields", ["assay_chembl_id", "molecule_chembl_id", "standard_type", "standard_relation", "standard_value"])

        for field in required_fields:
            if field in df.columns:
                missing_mask = df[field].isna()
                if missing_mask.any():
                    accepted_mask &= ~missing_mask
                    rejection_reasons.extend([f"missing_{field}" for _ in range(missing_mask.sum())])
                    logger.debug(f"Rejected {missing_mask.sum()} records due to missing {field}")

        # Check allowed activity types
        if "standard_type" in df.columns:
            allowed_types = self.strict_activity_types
            invalid_type_mask = df["standard_type"].notna() & ~df["standard_type"].isin(allowed_types)
            if invalid_type_mask.any():
                accepted_mask &= ~invalid_type_mask
                rejection_reasons.extend([f"invalid_activity_type_{row['standard_type']}" for _, row in df[invalid_type_mask].iterrows()])
                logger.debug(f"Rejected {invalid_type_mask.sum()} records due to invalid activity type")

        # Check allowed relations
        allowed_relations = self.strict_config.get("allowed_relations", ["="])
        if "standard_relation" in df.columns:
            invalid_relation_mask = df["standard_relation"].notna() & ~df["standard_relation"].isin(allowed_relations)
            if invalid_relation_mask.any():
                accepted_mask &= ~invalid_relation_mask
                rejection_reasons.extend([f"invalid_relation_{row['standard_relation']}" for _, row in df[invalid_relation_mask].iterrows()])
                logger.debug(f"Rejected {invalid_relation_mask.sum()} records due to invalid relation")

        # Check data validity comments (must be null for strict)
        if "data_validity_comment" in df.columns:
            has_validity_comment = df["data_validity_comment"].notna()
            if has_validity_comment.any():
                accepted_mask &= ~has_validity_comment
                rejection_reasons.extend([f"has_validity_comment_{row['data_validity_comment']}" for _, row in df[has_validity_comment].iterrows()])
                logger.debug(f"Rejected {has_validity_comment.sum()} records due to validity comments")

        # Check activity comments (no rejected values)
        if "activity_comment" in df.columns:
            rejected_comment_mask = df["activity_comment"].isin(self.rejected_activity_comments)
            if rejected_comment_mask.any():
                accepted_mask &= ~rejected_comment_mask
                rejection_reasons.extend([f"rejected_activity_comment_{row['activity_comment']}" for _, row in df[rejected_comment_mask].iterrows()])
                logger.debug(f"Rejected {rejected_comment_mask.sum()} records due to rejected activity comments")

        # Split data
        accepted_df = df[accepted_mask].copy()
        rejected_df = df[~accepted_mask].copy()

        # Add rejection reasons to rejected data
        if not rejected_df.empty:
            rejected_df["rejection_reason"] = rejection_reasons[: len(rejected_df)]

        logger.info(f"Strict quality filter: {len(accepted_df)} accepted, {len(rejected_df)} rejected")
        return accepted_df, rejected_df

    def apply_moderate_quality_filter(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Apply moderate quality filter and return (accepted, rejected) dataframes."""
        logger.info(f"Applying moderate quality filter to {len(df)} records")
        
        if not self.moderate_config.get('enabled', True):
            logger.info("Moderate quality filter is disabled")
            return df, pd.DataFrame()

        # Start with all records
        accepted_mask = pd.Series([True] * len(df), index=df.index)
        rejection_reasons = []

        # Check required fields (fewer requirements than strict)
        required_fields = self.moderate_config.get("required_fields", ["standard_type", "standard_value"])

        for field in required_fields:
            if field in df.columns:
                missing_mask = df[field].isna()
                if missing_mask.any():
                    accepted_mask &= ~missing_mask
                    rejection_reasons.extend([f"missing_{field}" for _ in range(missing_mask.sum())])
                    logger.debug(f"Rejected {missing_mask.sum()} records due to missing {field}")

        # For moderate profile, we allow unknown types and relations but mark them
        if "standard_type" in df.columns:
            unknown_type_mask = df["standard_type"].notna() & ~df["standard_type"].isin(self.strict_activity_types)
            if unknown_type_mask.any():
                # Mark as warning but don't reject - quality flags исключены из вывода
                # df.loc[unknown_type_mask, 'quality_flag'] = 'warning'
                # df.loc[unknown_type_mask, 'quality_reason'] = 'unknown_activity_type'
                logger.debug(f"Found {unknown_type_mask.sum()} records with unknown activity type (quality flags excluded from output)")

        # Check for allowed data validity warnings
        allowed_warnings = self.moderate_config.get("allowed_data_validity_warnings", [])
        if "data_validity_comment" in df.columns and allowed_warnings:
            invalid_warning_mask = df["data_validity_comment"].notna() & ~df["data_validity_comment"].isin(allowed_warnings)
            if invalid_warning_mask.any():
                accepted_mask &= ~invalid_warning_mask
                rejection_reasons.extend([f"invalid_validity_comment_{row['data_validity_comment']}" for _, row in df[invalid_warning_mask].iterrows()])
                logger.debug(f"Rejected {invalid_warning_mask.sum()} records due to invalid validity comments")

        # Split data
        accepted_df = df[accepted_mask].copy()
        rejected_df = df[~accepted_mask].copy()

        # Add rejection reasons to rejected data
        if not rejected_df.empty:
            rejected_df["rejection_reason"] = rejection_reasons[: len(rejected_df)]

        logger.info(f"Moderate quality filter: {len(accepted_df)} accepted, {len(rejected_df)} rejected")
        return accepted_df, rejected_df

    def apply_quality_profiles(self, df: pd.DataFrame) -> dict[str, Any]:
        """Apply both quality profiles and return results."""
        logger.info(f"Applying quality profiles to {len(df)} records")

        results = {"total_records": len(df), "strict_quality": {}, "moderate_quality": {}, "rejected": {}}

        # Apply strict quality filter
        strict_accepted, strict_rejected = self.apply_strict_quality_filter(df)
        results["strict_quality"] = {"accepted": strict_accepted, "rejected": strict_rejected, "accepted_count": len(strict_accepted), "rejected_count": len(strict_rejected)}

        # Apply moderate quality filter
        moderate_accepted, moderate_rejected = self.apply_moderate_quality_filter(df)
        results["moderate_quality"] = {
            "accepted": moderate_accepted,
            "rejected": moderate_rejected,
            "accepted_count": len(moderate_accepted),
            "rejected_count": len(moderate_rejected),
        }

        # Combine all rejected records
        all_rejected = pd.concat([strict_rejected, moderate_rejected], ignore_index=True)
        if not all_rejected.empty:
            # Remove duplicates while preserving rejection reasons
            all_rejected = all_rejected.drop_duplicates(subset=["activity_chembl_id"], keep="first")

        results["rejected"] = {"data": all_rejected, "count": len(all_rejected)}

        logger.info(f"Quality profiling completed: Strict: {len(strict_accepted)} accepted, Moderate: {len(moderate_accepted)} accepted, Total rejected: {len(all_rejected)}")

        return results

    def get_quality_statistics(self, df: pd.DataFrame) -> dict[str, Any]:
        """Generate quality statistics for the dataset."""
        logger.info("Generating quality statistics")

        stats = {"total_records": len(df), "quality_distribution": {}, "missing_data": {}, "foreign_key_coverage": {}, "censoring_statistics": {}}

        # Quality flag distribution - исключено из вывода
        # if 'quality_flag' in df.columns:
        #     quality_dist = df['quality_flag'].value_counts().to_dict()
        #     stats['quality_distribution'] = quality_dist

        # Missing data analysis
        critical_fields = ["activity_chembl_id", "standard_type", "standard_value", "assay_chembl_id", "molecule_chembl_id", "target_chembl_id", "document_chembl_id"]
        for field in critical_fields:
            if field in df.columns:
                missing_count = df[field].isna().sum()
                stats["missing_data"][field] = {"count": int(missing_count), "fraction": float(missing_count / len(df)) if len(df) > 0 else 0.0}

        # Foreign key coverage
        fk_fields = ["assay_chembl_id", "target_chembl_id", "document_chembl_id", "molecule_chembl_id"]
        for field in fk_fields:
            if field in df.columns:
                coverage_count = df[field].notna().sum()
                stats["foreign_key_coverage"][field] = {"count": int(coverage_count), "fraction": float(coverage_count / len(df)) if len(df) > 0 else 0.0}

        # Censoring statistics
        if "is_censored" in df.columns:
            censored_count = df["is_censored"].sum()
            stats["censoring_statistics"] = {
                "censored_count": int(censored_count),
                "censored_fraction": float(censored_count / len(df)) if len(df) > 0 else 0.0,
                "non_censored_count": int(len(df) - censored_count),
                "non_censored_fraction": float((len(df) - censored_count) / len(df)) if len(df) > 0 else 0.0,
            }

        # Activity type distribution
        if "standard_type" in df.columns:
            type_dist = df["standard_type"].value_counts().to_dict()
            stats["activity_type_distribution"] = type_dist

        # Relation distribution
        if "standard_relation" in df.columns:
            relation_dist = df["standard_relation"].value_counts().to_dict()
            stats["relation_distribution"] = relation_dist

        # Units distribution
        if "standard_units" in df.columns:
            units_dist = df["standard_units"].value_counts().to_dict()
            stats["units_distribution"] = units_dist

        logger.info("Quality statistics generated")
        return stats

    def validate_quality_thresholds(self, stats: dict[str, Any], thresholds: dict[str, float] | None = None) -> dict[str, Any]:
        """Validate quality statistics against thresholds."""
        if thresholds is None:
            thresholds = {"max_missing_fraction": 0.02, "max_duplicate_fraction": 0.005, "min_foreign_key_coverage": 0.95}

        validation_results = {"passed": True, "violations": []}

        # Check missing data thresholds
        max_missing = thresholds.get("max_missing_fraction", 0.02)
        for field, missing_stats in stats.get("missing_data", {}).items():
            if missing_stats["fraction"] > max_missing:
                validation_results["passed"] = False
                validation_results["violations"].append({"type": "missing_data", "field": field, "actual": missing_stats["fraction"], "threshold": max_missing})

        # Check foreign key coverage thresholds
        min_coverage = thresholds.get("min_foreign_key_coverage", 0.95)
        for field, coverage_stats in stats.get("foreign_key_coverage", {}).items():
            if coverage_stats["fraction"] < min_coverage:
                validation_results["passed"] = False
                validation_results["violations"].append({"type": "low_coverage", "field": field, "actual": coverage_stats["fraction"], "threshold": min_coverage})

        return validation_results
