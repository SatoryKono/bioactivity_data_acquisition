"""Common utilities for ChEMBL Pandera schema definitions."""

from __future__ import annotations

import pandas as pd
import pandera as pa
from pandera import Check, Column, DataFrameSchema

# Regex patterns for identifiers
CHEMBL_ID_PATTERN = r"^CHEMBL\d+$"
BAO_ID_PATTERN = r"^BAO_\d{7}$"
DOI_PATTERN = r"^10\.\d{4,9}/\S+$"

# InChI key length (fixed 27 characters)
INCHI_KEY_LENGTH = 27

# Hash length (BLAKE2-256 produces 64 character hex strings)
HASH_LENGTH = 64


def chembl_id_column(
    *,
    nullable: bool = True,
    unique: bool = False,
) -> Column:
    """Create a Column definition for ChEMBL ID fields.

    Args:
        nullable: Whether the column can contain null values
        unique: Whether values must be unique

    Returns:
        Column definition with ChEMBL ID validation
    """
    checks = [Check.str_matches(CHEMBL_ID_PATTERN)]  # type: ignore[arg-type]
    return Column(pa.String, checks=checks, nullable=nullable, unique=unique)  # type: ignore[assignment]


def bao_id_column(
    *,
    nullable: bool = True,
) -> Column:
    """Create a Column definition for BAO ID fields.

    Args:
        nullable: Whether the column can contain null values

    Returns:
        Column definition with BAO ID validation
    """
    checks = [Check.str_matches(BAO_ID_PATTERN)]  # type: ignore[arg-type]
    return Column(pa.String, checks=checks, nullable=nullable)  # type: ignore[assignment]


def doi_column(
    *,
    nullable: bool = True,
) -> Column:
    """Create a Column definition for DOI fields.

    Args:
        nullable: Whether the column can contain null values

    Returns:
        Column definition with DOI validation
    """
    checks = [Check.str_matches(DOI_PATTERN)]  # type: ignore[arg-type]
    return Column(pa.String, checks=checks, nullable=nullable)  # type: ignore[assignment]


def tax_id_column(
    *,
    nullable: bool = True,
) -> Column:
    """Create a Column definition for taxonomy ID fields (Int64 >= 1).

    Args:
        nullable: Whether the column can contain null values

    Returns:
        Column definition with tax ID validation
    """
    checks = [Check.ge(1)]  # type: ignore[arg-type]
    return Column(pa.Int64, checks=checks, nullable=nullable)  # type: ignore[assignment]


def boolean_flag_column(
    *,
    nullable: bool = True,
) -> Column:
    """Create a Column definition for boolean flag fields (0 or 1).

    Args:
        nullable: Whether the column can contain null values

    Returns:
        Column definition with boolean flag validation (pd.Int64Dtype)
    """
    checks = [Check.isin([0, 1])]  # type: ignore[arg-type]
    return Column(pd.Int64Dtype(), checks=checks, nullable=nullable)  # type: ignore[assignment]


def non_negative_float_column(
    *,
    nullable: bool = True,
) -> Column:
    """Create a Column definition for non-negative float fields.

    Args:
        nullable: Whether the column can contain null values

    Returns:
        Column definition with non-negative float validation
    """
    checks = [Check.ge(0)]  # type: ignore[arg-type]
    return Column(pa.Float64, checks=checks, nullable=nullable)  # type: ignore[assignment]


def non_negative_int_column(
    *,
    nullable: bool = True,
    dtype: type[pa.Int64] | type[pd.Int64Dtype] = pa.Int64,
) -> Column:
    """Create a Column definition for non-negative integer fields.

    Args:
        nullable: Whether the column can contain null values
        dtype: Either pa.Int64 (default) or pd.Int64Dtype for nullable integers

    Returns:
        Column definition with non-negative integer validation
    """
    checks = [Check.ge(0)]  # type: ignore[arg-type]
    return Column(dtype, checks=checks, nullable=nullable)  # type: ignore[assignment]


def positive_int_column(
    *,
    nullable: bool = True,
    dtype: type[pa.Int64] | type[pd.Int64Dtype] = pa.Int64,
) -> Column:
    """Create a Column definition for positive integer fields (>= 1).

    Args:
        nullable: Whether the column can contain null values
        dtype: Either pa.Int64 (default) or pd.Int64Dtype for nullable integers

    Returns:
        Column definition with positive integer validation
    """
    checks = [Check.ge(1)]  # type: ignore[arg-type]
    return Column(dtype, checks=checks, nullable=nullable)  # type: ignore[assignment]


def standard_string_column(
    *,
    nullable: bool = True,
) -> Column:
    """Create a Column definition for standard string fields.

    Args:
        nullable: Whether the column can contain null values

    Returns:
        Column definition for string fields
    """
    return Column(pa.String, nullable=nullable)  # type: ignore[assignment]


def inchi_key_column(
    *,
    nullable: bool = True,
    unique: bool = False,
) -> Column:
    """Create a Column definition for InChI key fields (27 characters).

    Args:
        nullable: Whether the column can contain null values
        unique: Whether values must be unique

    Returns:
        Column definition with InChI key length validation
    """
    checks = [Check.str_length(INCHI_KEY_LENGTH, INCHI_KEY_LENGTH)]  # type: ignore[arg-type]
    return Column(pa.String, checks=checks, nullable=nullable, unique=unique)  # type: ignore[assignment]


def hash_column(
    *,
    nullable: bool = False,
) -> Column:
    """Create a Column definition for hash fields (64 characters, BLAKE2-256).

    Args:
        nullable: Whether the column can contain null values

    Returns:
        Column definition with hash length validation
    """
    checks = [Check.str_length(HASH_LENGTH, HASH_LENGTH)]  # type: ignore[arg-type]
    return Column(pa.String, checks=checks, nullable=nullable)  # type: ignore[assignment]


def create_chembl_schema(
    columns: dict[str, Column],
    *,
    schema_name: str,
    version: str,
    strict: bool = False,
) -> DataFrameSchema:
    """Create a DataFrameSchema with standard ChEMBL schema parameters.

    Args:
        columns: Dictionary of column name to Column definition
        schema_name: Base name for the schema (e.g., "ActivitySchema")
        version: Schema version string (e.g., "1.5.0")
        strict: Whether to enforce strict column validation (default: False)

    Returns:
        DataFrameSchema with ordered=True, coerce=False, and versioned name
    """
    name = f"{schema_name}_v{version}"
    return DataFrameSchema(
        columns,
        strict=strict,
        ordered=True,
        coerce=False,  # Disable coercion at schema level - types are normalized in transform
        name=name,
    )


__all__ = [
    "CHEMBL_ID_PATTERN",
    "BAO_ID_PATTERN",
    "DOI_PATTERN",
    "INCHI_KEY_LENGTH",
    "HASH_LENGTH",
    "chembl_id_column",
    "bao_id_column",
    "doi_column",
    "tax_id_column",
    "boolean_flag_column",
    "non_negative_float_column",
    "non_negative_int_column",
    "positive_int_column",
    "standard_string_column",
    "inchi_key_column",
    "hash_column",
    "create_chembl_schema",
]

