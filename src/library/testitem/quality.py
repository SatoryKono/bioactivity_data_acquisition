"""Quality filtering and profiling for testitem data."""

from __future__ import annotations

import logging
import re
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


class TestitemQualityFilter:
    """Applies quality filters to testitem data."""

    def __init__(self, config: dict[str, Any] | None = None):
        """Initialize quality filter with configuration."""
        self.config = config or {}
        
        # Get quality profile settings
        self.quality_profiles = self.config.get('quality_profiles', {})
        self.strict_config = self.quality_profiles.get('strict', {})
        self.moderate_config = self.quality_profiles.get('moderate', {})
        
        # Get QC thresholds
        self.qc_thresholds = self.config.get('qc', {}).get('thresholds', {})

    def apply_strict_quality_filter(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Apply strict quality filter and return (accepted, rejected) dataframes."""
        logger.info(f"Applying strict quality filter to {len(df)} records")
        
        if not self.strict_config.get('enabled', True):
            logger.info("Strict quality filter is disabled")
            return df, pd.DataFrame()
        
        # Start with all records
        accepted_mask = pd.Series([True] * len(df), index=df.index)
        rejection_reasons = []
        
        # Check required fields
        required_fields = self.strict_config.get('required_fields', [
            'molecule_chembl_id'
        ])
        
        for field in required_fields:
            if field in df.columns:
                missing_mask = df[field].isna()
                if missing_mask.any():
                    accepted_mask &= ~missing_mask
                    rejection_reasons.extend([
                        f"missing_{field}" for _ in range(missing_mask.sum())
                    ])
                    logger.debug(f"Rejected {missing_mask.sum()} records due to missing {field}")
        
        # Check molecular weight validity
        if 'mw_freebase' in df.columns:
            invalid_mw_mask = (
                df['mw_freebase'].notna() & 
                ((df['mw_freebase'] <= 0) | (df['mw_freebase'] > 10000))
            )
            if invalid_mw_mask.any():
                accepted_mask &= ~invalid_mw_mask
                rejection_reasons.extend([
                    "invalid_molecular_weight" for _ in range(invalid_mw_mask.sum())
                ])
                logger.debug(f"Rejected {invalid_mw_mask.sum()} records due to invalid molecular weight")
        
        # Check ALogP validity
        if 'alogp' in df.columns:
            invalid_alogp_mask = (
                df['alogp'].notna() & 
                ((df['alogp'] < -10) | (df['alogp'] > 10))
            )
            if invalid_alogp_mask.any():
                accepted_mask &= ~invalid_alogp_mask
                rejection_reasons.extend([
                    "invalid_alogp" for _ in range(invalid_alogp_mask.sum())
                ])
                logger.debug(f"Rejected {invalid_alogp_mask.sum()} records due to invalid ALogP")
        
        # Check for duplicate molecule_chembl_id
        if 'molecule_chembl_id' in df.columns:
            duplicate_mask = df.duplicated(subset=['molecule_chembl_id'], keep=False)
            if duplicate_mask.any():
                accepted_mask &= ~duplicate_mask
                rejection_reasons.extend([
                    "duplicate_molecule_chembl_id" for _ in range(duplicate_mask.sum())
                ])
                logger.debug(f"Rejected {duplicate_mask.sum()} records due to duplicate molecule_chembl_id")
        
        # Check structure validity
        if 'standardized_smiles' in df.columns:
            invalid_smiles_mask = df['standardized_smiles'].notna() & ~df['standardized_smiles'].apply(self._validate_smiles)
            if invalid_smiles_mask.any():
                accepted_mask &= ~invalid_smiles_mask
                rejection_reasons.extend([
                    "invalid_smiles" for _ in range(invalid_smiles_mask.sum())
                ])
                logger.debug(f"Rejected {invalid_smiles_mask.sum()} records due to invalid SMILES")
        
        # Split data
        accepted_df = df[accepted_mask].copy()
        rejected_df = df[~accepted_mask].copy()
        
        # Add rejection reasons to rejected data
        if not rejected_df.empty:
            rejected_df['rejection_reason'] = rejection_reasons[:len(rejected_df)]
        
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
        required_fields = self.moderate_config.get('required_fields', [])
        
        for field in required_fields:
            if field in df.columns:
                missing_mask = df[field].isna()
                if missing_mask.any():
                    accepted_mask &= ~missing_mask
                    rejection_reasons.extend([
                        f"missing_{field}" for _ in range(missing_mask.sum())
                    ])
                    logger.debug(f"Rejected {missing_mask.sum()} records due to missing {field}")
        
        # For moderate profile, we allow some invalid molecular weights but mark them
        if 'mw_freebase' in df.columns:
            invalid_mw_mask = (
                df['mw_freebase'].notna() & 
                ((df['mw_freebase'] <= 0) | (df['mw_freebase'] > 10000))
            )
            if invalid_mw_mask.any():
                logger.debug(f"Found {invalid_mw_mask.sum()} records with invalid molecular weight (moderate mode - not rejected)")
        
        # Check for duplicate molecule_chembl_id (still reject duplicates)
        if 'molecule_chembl_id' in df.columns:
            duplicate_mask = df.duplicated(subset=['molecule_chembl_id'], keep=False)
            if duplicate_mask.any():
                accepted_mask &= ~duplicate_mask
                rejection_reasons.extend([
                    "duplicate_molecule_chembl_id" for _ in range(duplicate_mask.sum())
                ])
                logger.debug(f"Rejected {duplicate_mask.sum()} records due to duplicate molecule_chembl_id")
        
        # Split data
        accepted_df = df[accepted_mask].copy()
        rejected_df = df[~accepted_mask].copy()
        
        # Add rejection reasons to rejected data
        if not rejected_df.empty:
            rejected_df['rejection_reason'] = rejection_reasons[:len(rejected_df)]
        
        logger.info(f"Moderate quality filter: {len(accepted_df)} accepted, {len(rejected_df)} rejected")
        return accepted_df, rejected_df

    def build_quality_profile(self, df: pd.DataFrame) -> pd.DataFrame:
        """Build quality profile for testitem data."""
        logger.info("Building quality profile")
        
        metrics = []
        
        # Basic metrics
        metrics.append({"metric": "row_count", "value": len(df)})
        
        # Completeness metrics
        if 'molecule_chembl_id' in df.columns:
            missing_chembl_id = df['molecule_chembl_id'].isna().sum()
            metrics.append({"metric": "missing_molecule_chembl_id", "value": int(missing_chembl_id)})
        
        if 'pref_name' in df.columns:
            missing_pref_name = df['pref_name'].isna().sum()
            metrics.append({"metric": "missing_pref_name", "value": int(missing_pref_name)})
        
        if 'mw_freebase' in df.columns:
            missing_mw = df['mw_freebase'].isna().sum()
            metrics.append({"metric": "missing_molecular_weight", "value": int(missing_mw)})
        
        if 'alogp' in df.columns:
            missing_alogp = df['alogp'].isna().sum()
            metrics.append({"metric": "missing_alogp", "value": int(missing_alogp)})
        
        if 'standardized_smiles' in df.columns:
            missing_smiles = df['standardized_smiles'].isna().sum()
            metrics.append({"metric": "missing_smiles", "value": int(missing_smiles)})
        
        # Duplicate metrics
        if 'molecule_chembl_id' in df.columns:
            duplicates = df.duplicated(subset=['molecule_chembl_id']).sum()
            metrics.append({"metric": "duplicates", "value": int(duplicates)})
        
        # Validity metrics
        if 'mw_freebase' in df.columns:
            invalid_mw = df['mw_freebase'].notna() & ((df['mw_freebase'] <= 0) | (df['mw_freebase'] > 10000))
            metrics.append({"metric": "invalid_molecular_weight", "value": int(invalid_mw.sum())})
        
        if 'alogp' in df.columns:
            invalid_alogp = df['alogp'].notna() & ((df['alogp'] < -10) | (df['alogp'] > 10))
            metrics.append({"metric": "invalid_alogp", "value": int(invalid_alogp.sum())})
        
        if 'standardized_smiles' in df.columns:
            invalid_smiles = df['standardized_smiles'].notna() & ~df['standardized_smiles'].apply(self._validate_smiles)
            metrics.append({"metric": "invalid_smiles", "value": int(invalid_smiles.sum())})
        
        # Drug development metrics
        if 'max_phase' in df.columns:
            phase_distribution = df['max_phase'].value_counts()
            for phase, count in phase_distribution.items():
                metrics.append({"metric": f"max_phase_{phase}", "value": int(count)})
        
        if 'therapeutic_flag' in df.columns:
            therapeutic_count = df['therapeutic_flag'].sum() if df['therapeutic_flag'].dtype == bool else df['therapeutic_flag'].notna().sum()
            metrics.append({"metric": "therapeutic_compounds", "value": int(therapeutic_count)})
        
        # Structure coverage metrics
        structure_fields = ['standardized_smiles', 'standardized_inchi', 'standardized_inchi_key']
        for field in structure_fields:
            if field in df.columns:
                coverage = df[field].notna().sum()
                metrics.append({"metric": f"{field}_coverage", "value": int(coverage)})
        
        # Property coverage metrics
        property_fields = ['mw_freebase', 'alogp', 'hba', 'hbd', 'psa', 'rtb']
        for field in property_fields:
            if field in df.columns:
                coverage = df[field].notna().sum()
                metrics.append({"metric": f"{field}_coverage", "value": int(coverage)})
        
        # Source coverage metrics
        if 'source_system' in df.columns:
            source_distribution = df['source_system'].value_counts()
            for source, count in source_distribution.items():
                metrics.append({"metric": f"source_{source}_coverage", "value": int(count)})
        
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
            metric = row['metric']
            value = row['value']
            
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

    def _validate_smiles(self, smiles: str | None) -> bool:
        """Validate SMILES format."""
        if pd.isna(smiles) or not smiles:
            return True  # Empty SMILES is valid
        
        smiles_str = str(smiles).strip()
        
        # Basic SMILES validation - should contain common atoms and bonds
        valid_chars = set("CNOSPFClBrI()[]=#+-\\/")
        return all(c in valid_chars or c.isdigit() or c.islower() for c in smiles_str)

    def _validate_inchi(self, inchi: str | None) -> bool:
        """Validate InChI format."""
        if not inchi or pd.isna(inchi):
            return True  # Empty InChI is valid
        
        inchi_str = str(inchi).strip()
        return inchi_str.startswith("InChI=")

    def _validate_inchi_key(self, inchi_key: str | None) -> bool:
        """Validate InChIKey format."""
        if not inchi_key or pd.isna(inchi_key):
            return True  # Empty InChIKey is valid
        
        inchi_key_str = str(inchi_key).strip()
        return len(inchi_key_str) == 27 and inchi_key_str.count('-') == 2

    def validate_molecular_weight(self, mw: float | None) -> bool:
        """Validate molecular weight value."""
        if mw is None or pd.isna(mw):
            return True  # Empty MW is valid
        
        # Molecular weight should be positive and reasonable
        return 0 < mw < 10000  # Reasonable range for drug-like molecules

    def validate_alogp(self, alogp: float | None) -> bool:
        """Validate ALogP value."""
        if alogp is None or pd.isna(alogp):
            return True  # Empty ALogP is valid
        
        # ALogP should be within reasonable range
        return -10 <= alogp <= 10

    def validate_chembl_id_format(self, chembl_id: str | None) -> bool:
        """Validate ChEMBL ID format."""
        if not chembl_id or pd.isna(chembl_id):
            return True  # Empty ChEMBL ID is valid (nullable)
        
        # ChEMBL ID format: CHEMBL followed by digits
        chembl_pattern = r'^CHEMBL\d+$'
        return bool(re.match(chembl_pattern, str(chembl_id).strip()))

    def validate_drug_phase(self, phase: int | None) -> bool:
        """Validate drug development phase."""
        if phase is None or pd.isna(phase):
            return True  # Empty phase is valid
        
        # Phase should be between 0 and 4
        return 0 <= phase <= 4

    def validate_approval_year(self, year: int | None) -> bool:
        """Validate first approval year."""
        if year is None or pd.isna(year):
            return True  # Empty year is valid
        
        # Year should be reasonable
        return 1900 <= year <= 2030
