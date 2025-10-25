"""Unified validation system for the bioactivity data acquisition pipeline.

This module provides a comprehensive validation framework that consolidates
all validation logic across different entity types and stages.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Generic, TypeVar

import pandas as pd
import pandera as pa
from pandera import DataFrameSchema

from .exceptions import SchemaValidationError, ValidationError

logger = logging.getLogger(__name__)

T = TypeVar("T")


class ValidationStage(Enum):
    """Validation stages in the ETL pipeline."""

    INPUT = "input"  # Raw input data validation
    RAW = "raw"  # API response validation
    NORMALIZED = "normalized"  # Normalized data validation
    OUTPUT = "output"  # Final output validation


class ValidationSeverity(Enum):
    """Validation error severity levels."""

    WARNING = "warning"  # Non-blocking issues
    ERROR = "error"  # Blocking issues
    CRITICAL = "critical"  # Fatal issues


@dataclass
class ValidationResult:
    """Result of a validation operation."""

    is_valid: bool
    errors: list[ValidationError] = None
    warnings: list[str] = None
    stats: dict[str, Any] = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []
        if self.warnings is None:
            self.warnings = []
        if self.stats is None:
            self.stats = {}


class ValidationRule(ABC):
    """Abstract base class for validation rules."""

    @abstractmethod
    def validate(self, data: pd.DataFrame) -> ValidationResult:
        """Validate data against this rule."""
        pass

    @abstractmethod
    def get_description(self) -> str:
        """Get human-readable description of this rule."""
        pass


class SchemaValidationRule(ValidationRule):
    """Validation rule based on Pandera schema."""

    def __init__(self, schema: DataFrameSchema, stage: ValidationStage):
        self.schema = schema
        self.stage = stage

    def validate(self, data: pd.DataFrame) -> ValidationResult:
        """Validate data against Pandera schema."""
        try:
            validated_data = self.schema.validate(data, lazy=True)
            return ValidationResult(is_valid=True, stats={"rows_validated": len(validated_data)})
        except pa.errors.SchemaErrors as e:
            errors = []
            for failure_case in e.failure_cases.itertuples():
                error = SchemaValidationError(
                    f"Schema validation failed: {failure_case.failure_case}",
                    schema_name=self.schema.name or "unknown",
                    field_name=getattr(failure_case, "column", None),
                    record_id=str(getattr(failure_case, "index", None)),
                    validation_errors=[str(failure_case.failure_case)],
                )
                errors.append(error)

            return ValidationResult(is_valid=False, errors=errors, stats={"rows_validated": len(data), "errors_count": len(errors)})

    def get_description(self) -> str:
        return f"Schema validation for {self.stage.value} stage"


class CustomValidationRule(ValidationRule):
    """Custom validation rule with user-defined logic."""

    def __init__(self, name: str, validator_func: callable, severity: ValidationSeverity = ValidationSeverity.ERROR):
        self.name = name
        self.validator_func = validator_func
        self.severity = severity

    def validate(self, data: pd.DataFrame) -> ValidationResult:
        """Validate data using custom function."""
        try:
            result = self.validator_func(data)
            if isinstance(result, bool):
                is_valid = result
                errors = [] if result else [ValidationError(f"Custom validation '{self.name}' failed")]
            elif isinstance(result, ValidationResult):
                return result
            else:
                is_valid = True
                errors = []

            return ValidationResult(is_valid=is_valid, errors=errors, stats={"rule_name": self.name, "severity": self.severity.value})
        except Exception as e:
            error = ValidationError(f"Custom validation '{self.name}' raised exception: {str(e)}", cause=e)
            return ValidationResult(is_valid=False, errors=[error], stats={"rule_name": self.name, "exception": str(e)})

    def get_description(self) -> str:
        return f"Custom validation: {self.name}"


class EntityValidator(Generic[T]):
    """Validator for a specific entity type."""

    def __init__(self, entity_type: str):
        self.entity_type = entity_type
        self.rules: dict[ValidationStage, list[ValidationRule]] = {stage: [] for stage in ValidationStage}

    def add_rule(self, stage: ValidationStage, rule: ValidationRule) -> None:
        """Add a validation rule for a specific stage."""
        self.rules[stage].append(rule)

    def add_schema_rule(self, stage: ValidationStage, schema: DataFrameSchema) -> None:
        """Add a schema-based validation rule."""
        rule = SchemaValidationRule(schema, stage)
        self.add_rule(stage, rule)

    def add_custom_rule(self, stage: ValidationStage, name: str, validator_func: callable, severity: ValidationSeverity = ValidationSeverity.ERROR) -> None:
        """Add a custom validation rule."""
        rule = CustomValidationRule(name, validator_func, severity)
        self.add_rule(stage, rule)

    def validate(self, data: pd.DataFrame, stage: ValidationStage) -> ValidationResult:
        """Validate data for a specific stage."""
        logger.info(f"Validating {self.entity_type} data at {stage.value} stage")

        all_errors = []
        all_warnings = []
        combined_stats = {"entity_type": self.entity_type, "stage": stage.value}

        for rule in self.rules[stage]:
            result = rule.validate(data)

            if not result.is_valid:
                all_errors.extend(result.errors)

            all_warnings.extend(result.warnings)

            # Merge stats
            if result.stats:
                combined_stats.update(result.stats)

        is_valid = len(all_errors) == 0

        logger.info(
            f"Validation completed for {self.entity_type} at {stage.value} stage: {'PASSED' if is_valid else 'FAILED'} ({len(all_errors)} errors, {len(all_warnings)} warnings)"
        )

        return ValidationResult(is_valid=is_valid, errors=all_errors, warnings=all_warnings, stats=combined_stats)


class ValidationRegistry:
    """Registry for managing entity validators."""

    def __init__(self):
        self.validators: dict[str, EntityValidator] = {}

    def register_validator(self, entity_type: str, validator: EntityValidator) -> None:
        """Register a validator for an entity type."""
        self.validators[entity_type] = validator

    def get_validator(self, entity_type: str) -> EntityValidator | None:
        """Get validator for an entity type."""
        return self.validators.get(entity_type)

    def validate_entity(self, entity_type: str, data: pd.DataFrame, stage: ValidationStage) -> ValidationResult:
        """Validate data for a specific entity type and stage."""
        validator = self.get_validator(entity_type)
        if validator is None:
            error = ValidationError(f"No validator registered for entity type: {entity_type}")
            return ValidationResult(is_valid=False, errors=[error], stats={"entity_type": entity_type, "stage": stage.value})

        return validator.validate(data, stage)

    def list_entities(self) -> list[str]:
        """List all registered entity types."""
        return list(self.validators.keys())


# Global validation registry
_validation_registry = ValidationRegistry()


def get_validation_registry() -> ValidationRegistry:
    """Get the global validation registry."""
    return _validation_registry


def register_entity_validator(entity_type: str, validator: EntityValidator) -> None:
    """Register a validator for an entity type."""
    _validation_registry.register_validator(entity_type, validator)


def validate_entity_data(entity_type: str, data: pd.DataFrame, stage: ValidationStage) -> ValidationResult:
    """Validate data for a specific entity type and stage."""
    return _validation_registry.validate_entity(entity_type, data, stage)


# Common validation functions
def validate_required_columns(data: pd.DataFrame, required_columns: list[str]) -> bool:
    """Validate that all required columns are present."""
    missing_columns = set(required_columns) - set(data.columns)
    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        return False
    return True


def validate_no_duplicates(data: pd.DataFrame, key_columns: list[str]) -> bool:
    """Validate that there are no duplicate records based on key columns."""
    if not key_columns:
        return True

    duplicates = data.duplicated(subset=key_columns)
    if duplicates.any():
        duplicate_count = duplicates.sum()
        logger.error(f"Found {duplicate_count} duplicate records based on columns: {key_columns}")
        return False
    return True


def validate_data_types(data: pd.DataFrame, expected_types: dict[str, str]) -> bool:
    """Validate that columns have expected data types."""
    for column, expected_type in expected_types.items():
        if column not in data.columns:
            continue

        actual_type = str(data[column].dtype)
        if expected_type not in actual_type:
            logger.error(f"Column '{column}' has type '{actual_type}', expected '{expected_type}'")
            return False

    return True


def validate_value_ranges(data: pd.DataFrame, column_ranges: dict[str, tuple[float, float]]) -> bool:
    """Validate that numeric columns are within expected ranges."""
    for column, (min_val, max_val) in column_ranges.items():
        if column not in data.columns:
            continue

        numeric_data = pd.to_numeric(data[column], errors="coerce")
        out_of_range = (numeric_data < min_val) | (numeric_data > max_val)

        if out_of_range.any():
            count = out_of_range.sum()
            logger.error(f"Column '{column}' has {count} values outside range [{min_val}, {max_val}]")
            return False

    return True


def validate_string_patterns(data: pd.DataFrame, column_patterns: dict[str, str]) -> bool:
    """Validate that string columns match expected patterns."""

    for column, pattern in column_patterns.items():
        if column not in data.columns:
            continue

        string_data = data[column].astype(str)
        pattern_matches = string_data.str.match(pattern, na=False)

        if not pattern_matches.all():
            count = (~pattern_matches).sum()
            logger.error(f"Column '{column}' has {count} values not matching pattern '{pattern}'")
            return False

    return True


# Factory functions for common validators
def create_entity_validator(entity_type: str) -> EntityValidator:
    """Create a new entity validator."""
    return EntityValidator(entity_type)


def create_schema_validator(entity_type: str, schemas: dict[ValidationStage, DataFrameSchema]) -> EntityValidator:
    """Create a validator with schema-based rules."""
    validator = create_entity_validator(entity_type)

    for stage, schema in schemas.items():
        validator.add_schema_rule(stage, schema)

    return validator


# Utility functions for backward compatibility
def validate_with_schema(data: pd.DataFrame, schema: DataFrameSchema, lazy: bool = True) -> pd.DataFrame:
    """Validate data with a Pandera schema (backward compatibility)."""
    try:
        return schema.validate(data, lazy=lazy)
    except pa.errors.SchemaErrors as e:
        logger.error(f"Schema validation failed: {e.failure_cases}")
        raise


def get_validation_errors(data: pd.DataFrame, schema: DataFrameSchema) -> list[str]:
    """Get validation errors for data against a schema."""
    try:
        schema.validate(data, lazy=True)
        return []
    except pa.errors.SchemaErrors as e:
        return [str(error) for error in e.failure_cases["failure_case"]]


def is_schema_valid(data: pd.DataFrame, schema: DataFrameSchema) -> bool:
    """Check if data is valid against a schema."""
    try:
        schema.validate(data, lazy=True)
        return True
    except pa.errors.SchemaErrors:
        return False
