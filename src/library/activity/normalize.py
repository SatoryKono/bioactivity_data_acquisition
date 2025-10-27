"""Data normalization for activity records."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

# Removed unused imports

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
        """Нормализация данных активности согласно бизнес-правилам.
        
        Преобразует сырые данные активности в нормализованный формат,
        добавляя интервальные поля для цензурированных данных, внешние ключи
        для связей с измерениями и флаги качества.
        
        Args:
            df: DataFrame с валидированными сырыми данными активности
            
        Returns:
            pd.DataFrame: Нормализованный DataFrame с дополнительными полями:
                - lower_bound, upper_bound, is_censored: интервальное представление
                - assay_key, target_key, document_key, testitem_key: внешние ключи
                - quality_flag, quality_reason: оценка качества данных
                
        Example:
            >>> normalizer = ActivityNormalizer(config)
            >>> normalized_df = normalizer.normalize_activities(validated_df)
        """
        logger.info(f"Normalizing {len(df)} activity records")
        
        # Create a copy to avoid modifying original
        normalized_df = df.copy()
        
        # Apply schema-based normalization first
        normalized_df = self._apply_schema_normalizations(normalized_df)
        
        # Add computed fields
        normalized_df = self._add_interval_fields(normalized_df)
        normalized_df = self._add_foreign_keys(normalized_df)
        normalized_df = self._add_quality_flags(normalized_df)
        normalized_df = self._add_activity_aliases(normalized_df)
        normalized_df = self._calculate_pchembl_value(normalized_df)
        normalized_df = self._add_metadata_fields(normalized_df)
        normalized_df = self._add_missing_config_columns(normalized_df)
        
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
        
        # Create missing standard fields if they don't exist (these should come from ChEMBL)
        if 'standard_relation' not in df.columns:
            # If standard_relation is missing, try to derive from published_relation
            if 'published_relation' in df.columns:
                df['standard_relation'] = df['published_relation']
            else:
                df['standard_relation'] = '='  # Default relation
        if 'standard_value' not in df.columns:
            # If standard_value is missing, try to derive from published_value or activity_value
            if 'published_value' in df.columns:
                df['standard_value'] = df['published_value']
            elif 'activity_value' in df.columns:
                df['standard_value'] = df['activity_value']
            else:
                df['standard_value'] = None
        if 'standard_type' not in df.columns:
            # If standard_type is missing, try to derive from published_type or activity_type
            if 'published_type' in df.columns:
                df['standard_type'] = df['published_type']
            elif 'activity_type' in df.columns:
                df['standard_type'] = df['activity_type']
            else:
                df['standard_type'] = None
        
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
        validity_issues = pd.Series([False] * len(df), index=df.index)
        if 'data_validity_comment' in df.columns:
            validity_issues = df['data_validity_comment'].notna()
            if validity_issues.any():
                df.loc[validity_issues, 'quality_flag'] = 'warning'
                df.loc[validity_issues, 'quality_reason'] = 'data_validity_comment'
        
        # Check for activity comment issues
        activity_issues = pd.Series([False] * len(df), index=df.index)
        if 'activity_comment' in df.columns:
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
        censored_mask = df['is_censored']
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
        non_censored_mask = ~df['is_censored']
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
    
    def _add_activity_aliases(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add activity_type, activity_value, activity_unit as aliases for published fields."""
        logger.debug("Adding activity aliases from published fields")
        
        # activity_type = published_type (только если activity_type пустой)
        if 'published_type' in df.columns and 'activity_type' in df.columns:
            # Заполняем только пустые значения activity_type
            mask = df['activity_type'].isna() & df['published_type'].notna()
            filled_count = mask.sum()
            if filled_count > 0:
                df.loc[mask, 'activity_type'] = df.loc[mask, 'published_type']
                logger.debug(f"Filled {filled_count} empty activity_type values from published_type")
        
        # activity_value = published_value (только если activity_value пустой)
        if 'published_value' in df.columns and 'activity_value' in df.columns:
            # Заполняем только пустые значения activity_value
            mask = df['activity_value'].isna() & df['published_value'].notna()
            filled_count = mask.sum()
            if filled_count > 0:
                df.loc[mask, 'activity_value'] = df.loc[mask, 'published_value']
                logger.debug(f"Filled {filled_count} empty activity_value values from published_value")
        
        # activity_unit = published_units (только если activity_unit пустой)
        if 'published_units' in df.columns and 'activity_unit' in df.columns:
            # Заполняем только пустые значения activity_unit
            mask = df['activity_unit'].isna() & df['published_units'].notna()
            filled_count = mask.sum()
            if filled_count > 0:
                df.loc[mask, 'activity_unit'] = df.loc[mask, 'published_units']
                logger.debug(f"Filled {filled_count} empty activity_unit values from published_units")
        
        # Логируем статистику по activity_* полям
        if 'activity_type' in df.columns:
            empty_activity_type = df['activity_type'].isna().sum()
            logger.debug(f"Activity aliases summary: {empty_activity_type} empty activity_type values")
        if 'activity_value' in df.columns:
            empty_activity_value = df['activity_value'].isna().sum()
            logger.debug(f"Activity aliases summary: {empty_activity_value} empty activity_value values")
        if 'activity_unit' in df.columns:
            empty_activity_unit = df['activity_unit'].isna().sum()
            logger.debug(f"Activity aliases summary: {empty_activity_unit} empty activity_unit values")
        
        return df
    
    def _calculate_pchembl_value(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate pChEMBL values from standard_value."""
        import numpy as np
        
        if 'standard_value' in df.columns and 'standard_units' in df.columns:
            df['pchembl_value'] = None
            
            # Конвертация в моли
            unit_to_molar = {
                'nM': 1e-9,
                'uM': 1e-6,
                'mM': 1e-3,
                'M': 1.0
            }
            
            for unit, factor in unit_to_molar.items():
                mask = (df['standard_units'] == unit) & df['standard_value'].notna()
                if mask.any():
                    # Конвертировать в числовой формат
                    try:
                        numeric_values = pd.to_numeric(df.loc[mask, 'standard_value'], errors='coerce')
                        molar_values = numeric_values * factor
                        # Защита от log(0) и отрицательных значений
                        valid_mask = molar_values > 0
                        if valid_mask.any():
                            df.loc[mask & valid_mask, 'pchembl_value'] = -np.log10(molar_values[valid_mask])
                    except Exception as e:
                        logger.warning(f"Failed to calculate pchembl_value for unit {unit}: {e}")
        
        return df
    
    def _add_metadata_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add metadata fields to the DataFrame using unified utility."""
        from library.common.metadata_fields import add_system_metadata_fields
        
        # Используем унифицированную утилиту без ChEMBL клиента
        return add_system_metadata_fields(df, self.config, None)
    
    def _calculate_row_hash(self, row: pd.Series) -> str:
        """Calculate SHA256 hash of a DataFrame row."""
        import hashlib
        
        # Создать строку из всех значений строки
        row_string = '|'.join([str(val) if pd.notna(val) else '' for val in row.values])
        
        # Вычислить хеш
        return hashlib.sha256(row_string.encode('utf-8')).hexdigest()
    
    def _apply_schema_normalizations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Применяет функции нормализации из схемы к DataFrame.
        
        Args:
            df: DataFrame для нормализации
            
        Returns:
            DataFrame с примененными нормализациями
        """
        logger.info("Applying schema-based normalizations")
        
        # Применяем нормализации согласно спецификации
        df = self._normalize_relation_fields(df)
        df = self._normalize_text_fields(df)
        df = self._normalize_ontology_fields(df)
        df = self._normalize_units_fields(df)
        df = self._normalize_comment_fields(df)
        
        return df

    def _normalize_relation_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Нормализация полей relation."""
        logger.debug("Normalizing relation fields")
        
        relation_fields = ['relation', 'standard_relation', 'activity_prop_relation', 'activity_prop_standard_relation']
        valid_relations = ['=', '>', '>=', '<', '<=', '~']
        
        for field in relation_fields:
            if field in df.columns:
                # Нормализация к допустимым значениям
                df[field] = df[field].apply(lambda x: x if x in valid_relations else None if pd.isna(x) else '=')
        
        return df

    def _normalize_text_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Нормализация текстовых полей (trim)."""
        logger.debug("Normalizing text fields")
        
        text_fields = [
            'type', 'standard_type', 'activity_comment', 'text_value', 'standard_text_value',
            'activity_prop_type', 'activity_prop_standard_type', 'activity_prop_text_value',
            'activity_prop_standard_text_value', 'activity_prop_comments', 'action_type'
        ]
        
        for field in text_fields:
            if field in df.columns:
                df[field] = df[field].astype(str).str.strip().replace(['None', 'nan', 'NaN', 'none', 'NULL', 'null'], pd.NA)
        
        return df

    def _normalize_ontology_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Нормализация полей онтологий (upper)."""
        logger.debug("Normalizing ontology fields")
        
        ontology_fields = ['bao_endpoint', 'uo_units']
        
        for field in ontology_fields:
            if field in df.columns:
                df[field] = df[field].astype(str).str.upper().replace(['NONE', 'NAN', 'NULL'], pd.NA)
        
        return df

    def _normalize_units_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Нормализация полей единиц (trim + lower)."""
        logger.debug("Normalizing units fields")
        
        units_fields = ['units', 'standard_units', 'activity_prop_units', 'activity_prop_standard_units']
        
        for field in units_fields:
            if field in df.columns:
                df[field] = df[field].astype(str).str.strip().str.lower().replace(['none', 'nan', 'null', 'nil'], pd.NA)
        
        return df

    def _normalize_comment_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Нормализация полей комментариев (capitalize + trim)."""
        logger.debug("Normalizing comment fields")
        
        comment_fields = ['data_validity_comment']
        
        for field in comment_fields:
            if field in df.columns:
                df[field] = df[field].astype(str).str.strip().str.capitalize().replace(['None', 'Nan', 'Null'], pd.NA)
        
        # QUDT units - только trim
        if 'qudt_units' in df.columns:
            df['qudt_units'] = df['qudt_units'].astype(str).str.strip().replace(['None', 'nan', 'NaN', 'none', 'NULL', 'null'], pd.NA)
        
        return df
    
    def _add_missing_config_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add missing columns according to config column_order."""
        logger.debug("Adding missing columns from config")
        
        # Get column_order from config
        column_order = self.config.get('determinism', {}).get('column_order', [])
        if not column_order:
            logger.warning("No column_order found in config")
            return df
        
        # Create mapping from ChEMBL API fields to config fields
        field_mapping = {
            # Basic activity fields
            'activity_id': 'activity_chembl_id',  # Use activity_chembl_id as activity_id
            'assay_id': 'assay_chembl_id',  # Use assay_chembl_id as assay_id  
            'doc_id': 'document_chembl_id',  # Use document_chembl_id as doc_id
            'record_id': None,  # Not available from ChEMBL API, will be NULL
            'molregno': 'molecule_chembl_id',  # Use molecule_chembl_id as molregno
            
            # Activity data fields
            'type': 'published_type',
            'relation': 'published_relation', 
            'value': 'published_value',
            'units': 'published_units',
            'text_value': None,  # Not available, will be NULL
            'upper_value': None,  # Not available, will be NULL
            
            # Standardized fields (already exist)
            'standard_type': 'standard_type',
            'standard_relation': 'standard_relation',
            'standard_value': 'standard_value', 
            'standard_units': 'standard_units',
            'standard_flag': 'standard_flag',
            'standard_text_value': None,  # Not available, will be NULL
            'standard_upper_value': None,  # Not available, will be NULL
            
            # Comments and metadata
            'activity_comment': 'activity_comment',
            'data_validity_comment': 'data_validity_comment',
            'potential_duplicate': None,  # Not available, will be NULL
            'pchembl_value': 'pchembl_value',
            
            # Ontology fields
            'bao_endpoint': 'bao_endpoint',
            'uo_units': None,  # Not available, will be NULL
            'qudt_units': None,  # Not available, will be NULL
            'src_id': None,  # Not available, will be NULL
            'action_type': None,  # Not available, will be NULL
            
            # Activity properties (not available from ChEMBL API)
            'activity_prop_type': None,
            'activity_prop_relation': None,
            'activity_prop_value': None,
            'activity_prop_units': None,
            'activity_prop_text_value': None,
            'activity_prop_standard_type': None,
            'activity_prop_standard_relation': None,
            'activity_prop_standard_value': None,
            'activity_prop_standard_units': None,
            'activity_prop_standard_text_value': None,
            'activity_prop_comments': None,
            'activity_prop_result_flag': None,
            
            # Ligand efficiency (not available from ChEMBL API)
            'bei': None,
            'sei': None,
            'le': None,
            'lle': None,
            
            # System fields (already added by metadata)
            'index': 'index',
            'pipeline_version': 'pipeline_version',
            'source_system': 'source_system',
            'chembl_release': 'chembl_release',
            'extracted_at': 'extracted_at',
            'hash_row': 'hash_row',
            'hash_business_key': 'hash_business_key',
        }
        
        # Add missing columns
        for config_col in column_order:
            if config_col not in df.columns:
                source_col = field_mapping.get(config_col)
                if source_col and source_col in df.columns:
                    # Copy from source column
                    df[config_col] = df[source_col]
                    logger.debug(f"Added column {config_col} from {source_col}")
                else:
                    # Add as NULL
                    df[config_col] = None
                    logger.debug(f"Added column {config_col} as NULL")
        
        return df
