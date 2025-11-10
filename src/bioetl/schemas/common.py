"""Common column factory functions for Pandera schemas."""

from __future__ import annotations

from collections.abc import Iterable
from functools import partial

import pandas as pd
import pandera.pandas as pa
from pandera.pandas import Check, Column

# ChEMBL ID pattern
CHEMBL_ID_PATTERN = r"^CHEMBL\d+$"

# BAO ID pattern
BAO_ID_PATTERN = r"^BAO_\d{7}$"

# DOI pattern
DOI_PATTERN = r"^10\.\d{4,9}/\S+$"

# UUID pattern (lower/upper case supported)
UUID_PATTERN = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"


def make_string_column(
    *,
    nullable: bool = True,
    pattern: str | None = None,
    unique: bool = False,
    checks: Iterable[Check] | None = None,
) -> Column:
    """Create a string column with optional pattern and custom checks."""

    computed_checks: list[Check] = []
    if checks is not None:
        computed_checks.extend(checks)
    if pattern is not None:
        computed_checks.append(Check.str_matches(pattern))  # type: ignore[arg-type]

    return Column(
        pa.String,
        checks=computed_checks or None,  # type: ignore[arg-type,assignment]
        nullable=nullable,
        unique=unique,
    )


def string_column(
    *,
    nullable: bool,
    pattern: str | None = None,
    unique: bool = False,
) -> Column:
    """Create a string column with optional pattern constraint.

    Parameters
    ----------
    nullable
        Whether the column can contain null values.
    pattern
        Regex pattern column values must satisfy.
    unique
        Whether the column values must be unique.

    Returns
    -------
    Column
        A Pandera Column definition for strings.
    """
    return make_string_column(nullable=nullable, pattern=pattern, unique=unique)


def object_column(*, nullable: bool) -> Column:
    """Create an object column with nullable control."""

    return Column(pa.Object, nullable=nullable)  # type: ignore[assignment]


chembl_id_column = partial(make_string_column, pattern=CHEMBL_ID_PATTERN)
chembl_id_column.__doc__ = "Create a ChEMBL ID column with validation."


nullable_string_column = partial(make_string_column, nullable=True)
nullable_string_column.__doc__ = "Create a nullable string column."


non_nullable_string_column = partial(make_string_column, nullable=False)
non_nullable_string_column.__doc__ = "Create a non-nullable string column."


def nullable_int64_column(
    *,
    ge: int | None = None,
    isin: set[int] | None = None,
    le: int | None = None,
) -> Column:
    """Create a nullable Int64 column with optional constraints.

    Parameters
    ----------
    ge
        Minimum value (greater than or equal).
    isin
        Set of allowed values.
    le
        Maximum value (less than or equal).

    Returns
    -------
    Column
        A Pandera Column definition for nullable Int64.
    """
    checks: list[Check] = []
    if ge is not None:
        checks.append(Check.ge(ge))  # type: ignore[arg-type]
    if le is not None:
        checks.append(Check.le(le))  # type: ignore[arg-type]
    if isin is not None:
        checks.append(Check.isin(isin))  # type: ignore[arg-type]

    if checks:
        return Column(pa.Int64, checks=checks, nullable=True)  # type: ignore[assignment]
    return Column(pa.Int64, nullable=True)  # type: ignore[assignment]


def non_nullable_int64_column(
    *,
    ge: int | None = None,
    isin: set[int] | None = None,
    le: int | None = None,
    unique: bool = False,
) -> Column:
    """Create a non-nullable Int64 column with optional constraints.

    Parameters
    ----------
    ge
        Minimum value (greater than or equal).
    isin
        Set of allowed values.
    le
        Maximum value (less than or equal).
    unique
        Whether the column values must be unique.

    Returns
    -------
    Column
        A Pandera Column definition for non-nullable Int64.
    """
    checks: list[Check] = []
    if ge is not None:
        checks.append(Check.ge(ge))  # type: ignore[arg-type]
    if le is not None:
        checks.append(Check.le(le))  # type: ignore[arg-type]
    if isin is not None:
        checks.append(Check.isin(isin))  # type: ignore[arg-type]

    if checks:
        return Column(pa.Int64, checks=checks, nullable=False, unique=unique)  # type: ignore[assignment]
    return Column(pa.Int64, nullable=False, unique=unique)  # type: ignore[assignment]


def nullable_pd_int64_column(
    *,
    ge: int | None = None,
    isin: set[int] | None = None,
    le: int | None = None,
) -> Column:
    """Create a nullable pandas Int64Dtype column with optional constraints.

    Parameters
    ----------
    ge
        Minimum value (greater than or equal).
    isin
        Set of allowed values.
    le
        Maximum value (less than or equal).

    Returns
    -------
    Column
        A Pandera Column definition for nullable pandas Int64Dtype.
    """
    checks: list[Check] = []
    if ge is not None:
        checks.append(Check.ge(ge))  # type: ignore[arg-type]
    if le is not None:
        checks.append(Check.le(le))  # type: ignore[arg-type]
    if isin is not None:
        checks.append(Check.isin(isin))  # type: ignore[arg-type]

    if checks:
        return Column(pd.Int64Dtype(), checks=checks, nullable=True)  # type: ignore[assignment]
    return Column(pd.Int64Dtype(), checks=checks, nullable=True)  # type: ignore[assignment]


def nullable_float64_column(*, ge: float | None = None, le: float | None = None) -> Column:
    """Create a nullable Float64 column with optional constraints.

    Parameters
    ----------
    ge
        Minimum value (greater than or equal).
    le
        Maximum value (less than or equal).

    Returns
    -------
    Column
        A Pandera Column definition for nullable Float64.
    """
    checks: list[Check] = []
    if ge is not None:
        checks.append(Check.ge(ge))  # type: ignore[arg-type]
    if le is not None:
        checks.append(Check.le(le))  # type: ignore[arg-type]

    if checks:
        return Column(pa.Float64, checks=checks, nullable=True)  # type: ignore[assignment]
    return Column(pa.Float64, nullable=True)  # type: ignore[assignment]


def boolean_flag_column(*, use_boolean_dtype: bool = True) -> Column:
    """Create a boolean flag column.

    Parameters
    ----------
    use_boolean_dtype
        If True, use pd.BooleanDtype(). If False, use pd.Int64Dtype() with Check.isin([0, 1]).

    Returns
    -------
    Column
        A Pandera Column definition for boolean flags.
    """
    if use_boolean_dtype:
        return Column(pd.BooleanDtype(), nullable=True)  # type: ignore[assignment]
    return Column(pd.Int64Dtype(), Check.isin([0, 1]), nullable=True)  # type: ignore[arg-type,assignment]


def string_column_with_check(
    *,
    pattern: str | None = None,
    isin: set[str] | None = None,
    nullable: bool = True,
    unique: bool = False,
    str_length: tuple[int, int] | None = None,
) -> Column:
    """Create a string column with optional validation checks.

    Parameters
    ----------
    pattern
        Regex pattern to match.
    isin
        Set of allowed values.
    nullable
        Whether the column can contain null values.
    unique
        Whether the column values must be unique.
    str_length
        Tuple of (min_length, max_length) for string length validation.

    Returns
    -------
    Column
        A Pandera Column definition for strings with checks.
    """
    base_column = make_string_column(
        nullable=nullable,
        pattern=pattern,
        unique=unique,
    )
    initial_checks: list[Check] = list(base_column.checks or [])
    checks: list[Check] = list(initial_checks)
    if isin is not None:
        checks.append(Check.isin(isin))  # type: ignore[arg-type]
    if str_length is not None:
        checks.append(Check.str_length(str_length[0], str_length[1]))  # type: ignore[arg-type]

    if checks == initial_checks:
        return base_column
    return make_string_column(nullable=nullable, unique=unique, checks=checks)


def row_metadata_columns() -> dict[str, Column]:
    """Create row metadata columns (row_subtype, row_index).

    Returns
    -------
    dict[str, Column]
        Dictionary with 'row_subtype' and 'row_index' column definitions.
    """
    return {
        "row_subtype": non_nullable_string_column(),
        "row_index": non_nullable_int64_column(ge=0),
    }


bao_id_column = partial(make_string_column, pattern=BAO_ID_PATTERN)
bao_id_column.__doc__ = "Create a BAO ID column with validation."


doi_column = partial(make_string_column, pattern=DOI_PATTERN)
doi_column.__doc__ = "Create a DOI column with validation."


def nullable_object_column() -> Column:
    """Create a nullable object column.

    Returns
    -------
    Column
        A Pandera Column definition for nullable objects.
    """
    return object_column(nullable=True)


uuid_column = partial(make_string_column, pattern=UUID_PATTERN)
uuid_column.__doc__ = "Create a UUID column enforcing canonical hyphenated format."


__all__ = [
    "CHEMBL_ID_PATTERN",
    "BAO_ID_PATTERN",
    "DOI_PATTERN",
    "UUID_PATTERN",
    "make_string_column",
    "chembl_id_column",
    "nullable_string_column",
    "non_nullable_string_column",
    "nullable_int64_column",
    "non_nullable_int64_column",
    "nullable_pd_int64_column",
    "nullable_float64_column",
    "boolean_flag_column",
    "string_column_with_check",
    "string_column",
    "row_metadata_columns",
    "bao_id_column",
    "doi_column",
    "nullable_object_column",
    "object_column",
    "uuid_column",
]
