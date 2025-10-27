"""Data validation for activity records using Pandera schemas."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd
import pandera.pandas as pa
from .schemas.activity_schema import ActivityRawSchema, ActivityNormalizedSchema

logger = logging.getLogger(__name__)


class ActivityValidator:
    """Validates activity data using Pandera schemas."""

    def __init__(self, config: dict[str, Any] | None = None):
        """Initialize validator with configuration."""
        self.config = config or {}
        self.strict_mode = self.config.get("validation", {}).get("strict", True)

        # Get allowed values from config
        self.strict_activity_types = self.config.get("normalization", {}).get("strict_activity_types", ["IC50", "Ki"])

        self.rejected_activity_comments = self.config.get("normalization", {}).get("rejected_activity_comments", ["inconclusive", "undetermined", "unevaluated"])

    def get_raw_schema(self) -> DataFrameSchema:
        """Schema for raw activity data from ChEMBL API."""
        return ActivityRawSchema.get_schema()

    def get_normalized_schema(self) -> DataFrameSchema:
        """Schema for normalized activity data."""
        return ActivityNormalizedSchema.get_schema()

    def get_strict_quality_schema(self) -> DataFrameSchema:
        """Schema for strict quality profile."""
        base_schema = self.get_normalized_schema()

        # Override columns with strict requirements
        strict_columns = {
            **base_schema.columns,
            "assay_key": Column(
                pa.String,
                checks=[
                    Check.str_matches(r"^CHEMBL\d+$", name="chembl_id_format")
                ],
                nullable=False,  # Required for strict profile
                description="Foreign key to assay table (required)"
            ),
            "testitem_key": Column(
                pa.String,
                checks=[
                    Check.str_matches(r"^CHEMBL\d+$", name="chembl_id_format")
                ],
                nullable=False,  # Required for strict profile
                description="Foreign key to testitem table (required)"
            ),
            "standard_type": Column(
                pa.String,
                checks=[
                    Check.isin(self.strict_activity_types, name="strict_activity_type")
                ],
                nullable=False,  # Required for strict profile
                description="Standardized activity type (strict)",
            ),
            "standard_relation": Column(
                pa.String,
                checks=[
                    Check.isin(["="], name="strict_relation")
                ],
                nullable=False,  # Required for strict profile
                description="Standardized relation (strict)",
            ),
            "standard_value": Column(
                pa.Float,
                checks=[
                    Check.greater_than(0, name="positive_value")
                ],
                nullable=False,  # Required for strict profile
                description="Standardized activity value (required)",
            ),
            "data_validity_comment": Column(
                pa.String,
                nullable=True,
                description="Data validity comment (must be null for strict)"
            ),
            "activity_comment": Column(
                pa.String,
                nullable=True,
                description="Activity comment (no rejected values for strict)"
            ),
        }

        return DataFrameSchema(
            columns=strict_columns,
            strict=False,  # Разрешаем дополнительные колонки
            coerce=True,  # Автоматическое преобразование типов
            checks=base_schema.checks,
        )

    def get_moderate_quality_schema(self) -> DataFrameSchema:
        """Schema for moderate quality profile."""
        base_schema = self.get_normalized_schema()

        # Override columns with moderate requirements
        moderate_columns = {
            **base_schema.columns,
            "standard_type": Column(
                pa.String,
                nullable=False,  # Still required but no restriction on values
                description="Standardized activity type (required)",
            ),
            "standard_value": Column(
                pa.Float,
                checks=[Check.greater_than(0, name="positive_value")],
                nullable=False,  # Still required
                description="Standardized activity value (required)",
            ),
        }

        return DataFrameSchema(
            columns=moderate_columns,
            strict=False,  # Разрешаем дополнительные колонки
            coerce=True,  # Автоматическое приведение типов
            checks=base_schema.checks
        )

    def validate_raw_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Валидация сырых данных активности через Pandera схемы.

        Проверяет соответствие данных активности схеме RawBioactivitySchema,
        включая типы данных, обязательные поля и бизнес-правила.

        Args:
            df: DataFrame с сырыми данными активности из ChEMBL API

        Returns:
            pd.DataFrame: Валидированный DataFrame с теми же данными

        Raises:
            SchemaError: Если данные не соответствуют схеме в строгом режиме

        Example:
            >>> validator = ActivityValidator(config)
            >>> validated_df = validator.validate_raw_data(raw_df)
        """
        logger.info(f"Validating {len(df)} raw activity records")

        try:
            schema = self.get_raw_schema()
            validated_df = schema.validate(df)
            logger.info("Raw data validation passed")
            return validated_df
        except pa.errors.SchemaError as e:
            logger.error(f"Raw data validation failed: {e}")
            if self.strict_mode:
                raise
            else:
                logger.warning("Continuing with validation errors in non-strict mode")
                return df

    def validate_normalized_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate normalized activity data."""
        logger.info(f"Validating {len(df)} normalized activity records")

        try:
            schema = self.get_normalized_schema()
            validated_df = schema.validate(df)
            logger.info("Normalized data validation passed")
            return validated_df
        except pa.errors.SchemaError as e:
            logger.error(f"Normalized data validation failed: {e}")
            if self.strict_mode:
                raise
            else:
                logger.warning("Continuing with validation errors in non-strict mode")
                return df

    def validate_strict_quality(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate data against strict quality profile."""
        logger.info(f"Validating {len(df)} records against strict quality profile")

        try:
            schema = self.get_strict_quality_schema()
            validated_df = schema.validate(df)
            logger.info("Strict quality validation passed")
            return validated_df
        except pa.errors.SchemaError as e:
            logger.error(f"Strict quality validation failed: {e}")
            if self.strict_mode:
                raise
            else:
                logger.warning("Continuing with validation errors in non-strict mode")
                return df

    def validate_moderate_quality(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate data against moderate quality profile."""
        logger.info(f"Validating {len(df)} records against moderate quality profile")

        try:
            schema = self.get_moderate_quality_schema()
            validated_df = schema.validate(df)
            logger.info("Moderate quality validation passed")
            return validated_df
        except pa.errors.SchemaError as e:
            logger.error(f"Moderate quality validation failed: {e}")
            if self.strict_mode:
                raise
            else:
                logger.warning("Continuing with validation errors in non-strict mode")
                return df

    def _validate_bounds_relation_consistency(self, df: pd.DataFrame) -> bool:
        """Validate that bounds are consistent with relation."""
        try:
            # Check that relation matches bounds pattern
            for _, row in df.iterrows():
                relation = row.get("standard_relation")
                lower_bound = row.get("lower_bound")
                upper_bound = row.get("upper_bound")
                is_censored = row.get("is_censored", False)

                if pd.isna(relation):
                    continue

                if relation == "=":
                    # Exact value: both bounds should be equal to standard_value
                    if not (pd.notna(lower_bound) and pd.notna(upper_bound) and lower_bound == upper_bound and not is_censored):
                        return False
                elif relation in [">", ">="]:
                    # Greater than: only lower bound should be present
                    if not (pd.notna(lower_bound) and pd.isna(upper_bound) and is_censored):
                        return False
                elif relation in ["<", "<="]:
                    # Less than: only upper bound should be present
                    if not (pd.isna(lower_bound) and pd.notna(upper_bound) and is_censored):
                        return False

            return True
        except Exception:
            return False

    def get_validation_report(self, df: pd.DataFrame) -> dict[str, Any]:
        """Generate validation report for the dataset."""
        logger.info("Generating validation report")

        report = {"total_records": len(df), "validation_checks": {}}

        # Check for duplicates
        duplicates = df.duplicated(subset=["activity_chembl_id"]).sum()
        report["validation_checks"]["duplicates"] = {"count": int(duplicates), "fraction": float(duplicates / len(df)) if len(df) > 0 else 0.0}

        # Check missing values in critical fields
        critical_fields = ["activity_chembl_id", "standard_type", "standard_value"]
        for field in critical_fields:
            if field in df.columns:
                missing = df[field].isna().sum()
                report["validation_checks"][f"missing_{field}"] = {"count": int(missing), "fraction": float(missing / len(df)) if len(df) > 0 else 0.0}

        # Check foreign key coverage
        fk_fields = ["assay_key", "target_key", "document_key", "testitem_key"]
        for field in fk_fields:
            if field in df.columns:
                coverage = df[field].notna().sum()
                report["validation_checks"][f"{field}_coverage"] = {"count": int(coverage), "fraction": float(coverage / len(df)) if len(df) > 0 else 0.0}

        # Check quality distribution - исключено из вывода
        # if 'quality_flag' in df.columns:
        #     quality_dist = df['quality_flag'].value_counts().to_dict()
        #     report["validation_checks"]["quality_distribution"] = quality_dist

        # Check censoring distribution
        if "is_censored" in df.columns:
            censored_count = df["is_censored"].sum()
            report["validation_checks"]["censoring"] = {"censored_count": int(censored_count), "censored_fraction": float(censored_count / len(df)) if len(df) > 0 else 0.0}

        logger.info("Validation report generated")
        return report
