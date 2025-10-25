"""Quality filtering and profiling for document data."""

from __future__ import annotations

import logging
import re
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


class DocumentQualityFilter:
    """Applies quality filters to document data."""

    def __init__(self, config: dict[str, Any] | None = None):
        """Initialize quality filter with configuration."""
        self.config = config or {}

        # Get quality profile settings
        self.quality_profiles = self.config.get("quality_profiles", {})
        self.strict_config = self.quality_profiles.get("strict", {})
        self.moderate_config = self.quality_profiles.get("moderate", {})

        # Get QC thresholds
        self.qc_thresholds = self.config.get("qc", {}).get("thresholds", {})

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
        required_fields = self.strict_config.get("required_fields", ["document_chembl_id", "title"])

        for field in required_fields:
            if field in df.columns:
                missing_mask = df[field].isna()
                if missing_mask.any():
                    accepted_mask &= ~missing_mask
                    rejection_reasons.extend([f"missing_{field}" for _ in range(missing_mask.sum())])
                    logger.debug(f"Rejected {missing_mask.sum()} records due to missing {field}")

        # Check DOI format validity
        if "doi" in df.columns:
            invalid_doi_mask = df["doi"].notna() & ~df["doi"].str.match(r"^10\.\d+/.*", na=False)
            if invalid_doi_mask.any():
                accepted_mask &= ~invalid_doi_mask
                rejection_reasons.extend(["invalid_doi_format" for _ in range(invalid_doi_mask.sum())])
                logger.debug(f"Rejected {invalid_doi_mask.sum()} records due to invalid DOI format")

        # Check year validity
        if "year" in df.columns:
            invalid_year_mask = df["year"].notna() & ((df["year"] < 1900) | (df["year"] > 2030))
            if invalid_year_mask.any():
                accepted_mask &= ~invalid_year_mask
                rejection_reasons.extend(["invalid_year" for _ in range(invalid_year_mask.sum())])
                logger.debug(f"Rejected {invalid_year_mask.sum()} records due to invalid year")

        # Check for duplicate document_chembl_id
        if "document_chembl_id" in df.columns:
            duplicate_mask = df.duplicated(subset=["document_chembl_id"], keep=False)
            if duplicate_mask.any():
                accepted_mask &= ~duplicate_mask
                rejection_reasons.extend(["duplicate_document_chembl_id" for _ in range(duplicate_mask.sum())])
                logger.debug(f"Rejected {duplicate_mask.sum()} records due to duplicate document_chembl_id")

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

        if not self.moderate_config.get("enabled", True):
            logger.info("Moderate quality filter is disabled")
            return df, pd.DataFrame()

        # Start with all records
        accepted_mask = pd.Series([True] * len(df), index=df.index)
        rejection_reasons = []

        # Check required fields (fewer requirements than strict)
        required_fields = self.moderate_config.get("required_fields", ["document_chembl_id"])

        for field in required_fields:
            if field in df.columns:
                missing_mask = df[field].isna()
                if missing_mask.any():
                    accepted_mask &= ~missing_mask
                    rejection_reasons.extend([f"missing_{field}" for _ in range(missing_mask.sum())])
                    logger.debug(f"Rejected {missing_mask.sum()} records due to missing {field}")

        # For moderate profile, we allow some invalid DOIs but mark them
        if "doi" in df.columns:
            invalid_doi_mask = df["doi"].notna() & ~df["doi"].str.match(r"^10\.\d+/.*", na=False)
            if invalid_doi_mask.any():
                logger.debug(f"Found {invalid_doi_mask.sum()} records with invalid DOI format (moderate mode - not rejected)")

        # Check for duplicate document_chembl_id (still reject duplicates)
        if "document_chembl_id" in df.columns:
            duplicate_mask = df.duplicated(subset=["document_chembl_id"], keep=False)
            if duplicate_mask.any():
                accepted_mask &= ~duplicate_mask
                rejection_reasons.extend(["duplicate_document_chembl_id" for _ in range(duplicate_mask.sum())])
                logger.debug(f"Rejected {duplicate_mask.sum()} records due to duplicate document_chembl_id")

        # Split data
        accepted_df = df[accepted_mask].copy()
        rejected_df = df[~accepted_mask].copy()

        # Add rejection reasons to rejected data
        if not rejected_df.empty:
            rejected_df["rejection_reason"] = rejection_reasons[: len(rejected_df)]

        logger.info(f"Moderate quality filter: {len(accepted_df)} accepted, {len(rejected_df)} rejected")
        return accepted_df, rejected_df

    def build_quality_profile(self, df: pd.DataFrame) -> pd.DataFrame:
        """Build quality profile for document data."""
        logger.info("Building quality profile")

        metrics = []

        # Basic metrics
        metrics.append({"metric": "row_count", "value": len(df)})

        # Completeness metrics
        if "document_chembl_id" in df.columns:
            missing_chembl_id = df["document_chembl_id"].isna().sum()
            metrics.append({"metric": "missing_document_chembl_id", "value": int(missing_chembl_id)})

        if "title" in df.columns:
            missing_title = df["title"].isna().sum()
            metrics.append({"metric": "missing_title", "value": int(missing_title)})

        if "doi" in df.columns:
            missing_doi = df["doi"].isna().sum()
            metrics.append({"metric": "missing_doi", "value": int(missing_doi)})

        if "document_pubmed_id" in df.columns:
            missing_pmid = df["document_pubmed_id"].isna().sum()
            metrics.append({"metric": "missing_pmid", "value": int(missing_pmid)})

        # DOI and PMID together
        if "doi" in df.columns and "document_pubmed_id" in df.columns:
            missing_both = df["doi"].isna() & df["document_pubmed_id"].isna()
            metrics.append({"metric": "missing_doi_and_pmid", "value": int(missing_both.sum())})

        # Duplicate metrics
        if "document_chembl_id" in df.columns:
            duplicates = df.duplicated(subset=["document_chembl_id"]).sum()
            metrics.append({"metric": "duplicates", "value": int(duplicates)})

        # Validity metrics
        if "doi" in df.columns:
            invalid_doi = df["doi"].notna() & ~df["doi"].str.match(r"^10\.\d+/.*", na=False)
            metrics.append({"metric": "invalid_doi_fraction", "value": int(invalid_doi.sum())})

        if "year" in df.columns:
            invalid_year = df["year"].notna() & ((df["year"] < 1900) | (df["year"] > 2030))
            metrics.append({"metric": "year_out_of_range", "value": int(invalid_year.sum())})

        if "journal" in df.columns:
            # Simple journal validation - check for empty or very short names
            invalid_journal = df["journal"].notna() & (df["journal"].str.len() < 3)
            metrics.append({"metric": "invalid_journal_fraction", "value": int(invalid_journal.sum())})

        # Page logic errors
        if "first_page" in df.columns and "last_page" in df.columns:
            page_logic_errors = df["first_page"].notna() & df["last_page"].notna() & (df["first_page"] > df["last_page"])
            metrics.append({"metric": "page_logic_errors", "value": int(page_logic_errors.sum())})

        # Source coverage metrics
        source_columns = {
            "crossref": ["crossref_doi", "crossref_title"],
            "openalex": ["openalex_doi", "openalex_title"],
            "pubmed": ["pubmed_doi", "pubmed_title"],
            "semantic_scholar": ["semantic_scholar_doi", "semantic_scholar_title"],
            "chembl": ["chembl_doi", "chembl_title"],
        }

        for source, columns in source_columns.items():
            if all(col in df.columns for col in columns):
                # Check if any of the source columns have data
                has_data = df[columns].notna().any(axis=1)
                coverage = has_data.sum()
                metrics.append({"metric": f"{source}_coverage", "value": int(coverage)})

        return pd.DataFrame(metrics)

    def apply_qc_thresholds(self, qc_report: pd.DataFrame, df: pd.DataFrame) -> bool:
        """Apply QC thresholds and return True if all pass."""
        logger.info("Applying QC thresholds")

        if not self.qc_thresholds:
            logger.info("No QC thresholds configured")
            return True

        total_rows = len(df)
        failed_checks = []

        for _, row in qc_report.iterrows():
            metric = row["metric"]
            value = row["value"]

            if metric in self.qc_thresholds:
                threshold = self.qc_thresholds[metric]

                # Calculate ratio for percentage-based thresholds
                if isinstance(threshold, float) and 0 <= threshold <= 1:
                    ratio = value / total_rows if total_rows > 0 else 0
                    if ratio > threshold:
                        failed_checks.append(f"{metric}: {ratio:.3f} > {threshold}")
                else:
                    # Absolute threshold
                    if value > threshold:
                        failed_checks.append(f"{metric}: {value} > {threshold}")

        if failed_checks:
            logger.error(f"QC thresholds failed: {failed_checks}")
            return False

        logger.info("All QC thresholds passed")
        return True

    def validate_doi_format(self, doi: str | None) -> bool:
        """Validate DOI format."""
        if not doi or pd.isna(doi):
            return True  # Empty DOI is valid

        # Basic DOI format: 10.xxxx/xxxx
        doi_pattern = r"^10\.\d+/.*"
        return bool(re.match(doi_pattern, str(doi).strip()))

    def validate_journal_name(self, journal: str | None) -> bool:
        """Validate journal name quality."""
        if not journal or pd.isna(journal):
            return True  # Empty journal is valid

        journal_str = str(journal).strip()

        # Check minimum length
        if len(journal_str) < 3:
            return False

        # Check for common invalid patterns
        invalid_patterns = [
            r"^[0-9]+$",  # Only numbers
            r"^[^a-zA-Z]*$",  # No letters
            r"^(unknown|n/a|na|null|none)$",  # Common invalid values
        ]

        for pattern in invalid_patterns:
            if re.match(pattern, journal_str, re.IGNORECASE):
                return False

        return True

    def validate_publication_date(self, date: str | None) -> bool:
        """Validate publication date format."""
        if not date or pd.isna(date):
            return True  # Empty date is valid

        # Expected format: YYYY-MM-DD
        date_pattern = r"^\d{4}-\d{2}-\d{2}$"
        if not re.match(date_pattern, str(date)):
            return False

        # Additional validation could be added here (e.g., valid month/day ranges)
        return True
