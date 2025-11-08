"""Common validators, normalizers, and Pandera column factories.

This module now hosts two groups of helpers:

* lightweight, pure validation/normalization primitives that can be shared
  across ETL components; and
* Pandera column factory utilities that encode domain specific constraints for
  tabular payloads.

Both sets of helpers are deliberately deterministic and raise predictable
exceptions (``TypeError`` or ``ValueError``) to make upstream error handling
consistent.
"""

from __future__ import annotations

import unicodedata
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from decimal import Decimal
from typing import Any, TypeVar, overload
from urllib.parse import quote, unquote, urlparse, urlunparse

import pandas as pd
import pandera as pa
from pandera import Check, Column
from rfc3339_validator import validate_rfc3339
from rfc3986_validator import validate_rfc3986

T = TypeVar("T")


def _normalize_percent_encoded(value: str, *, safe: str) -> str:
    """Normalise percent-encoding for URL components.

    Parameters
    ----------
    value
        Raw component value.
    safe
        Characters that should remain unescaped.

    Returns
    -------
    str
        Component with canonical percent-encoding (uppercase hex digits and
        escaped where required).
    """

    # ``unquote`` collapses any redundant escapes, ``quote`` then reapplies the
    # canonical encoding using uppercase hex digits.
    return quote(unquote(value), safe=safe)


def ensure_iterable(obj: T | Iterable[T], *, allow_string: bool = False) -> Iterable[T]:
    """Return an iterable view of *obj*.

    Strings and ``bytes`` are treated as scalar values by default to avoid
    accidentally iterating over individual characters.

    Examples
    --------
    >>> list(ensure_iterable([1, 2, 3]))
    [1, 2, 3]
    >>> list(ensure_iterable(5))
    [5]
    >>> list(ensure_iterable("abc"))
    ['abc']

    Complexity
    ----------
    ``O(1)`` time and memory.
    """

    if isinstance(obj, (str, bytes)) and not allow_string:
        return (obj,)  # type: ignore[return-value]

    try:
        iterator = iter(obj)  # type: ignore[arg-type]
    except TypeError:
        return (obj,)  # type: ignore[return-value]

    if isinstance(obj, Iterable):
        return obj

    # ``obj`` supports ``iter`` via ``__getitem__``; return the iterator we
    # obtained to provide a usable iterable view.
    return iterator


def _identity(value: T) -> T:
    """Return *value* unchanged."""

    return value


def ensure_unique(
    items: Iterable[T], *, key: Callable[[T], Any] | None = None
) -> list[T]:
    """Verify that *items* contains no duplicates.

    Parameters
    ----------
    items
        Iterable to inspect.
    key
        Optional projection function used to derive a hashable uniqueness key.

    Returns
    -------
    list
        Original items materialised as a list when uniqueness holds.

    Raises
    ------
    ValueError
        If duplicate elements are encountered.

    Examples
    --------
    >>> ensure_unique([1, 2, 3])
    [1, 2, 3]

    Complexity
    ----------
    ``O(n)`` time, ``O(n)`` memory, where ``n`` is the number of items.
    """

    seen: set[Any] = set()
    key_func: Callable[[T], Any]
    key_func = _identity if key is None else key

    materialised = list(items)
    for item in materialised:
        try:
            uniqueness_key = key_func(item)
        except Exception as exc:  # pragma: no cover - defensive safeguard
            raise TypeError("uniqueness key computation failed") from exc

        try:
            already_seen = uniqueness_key in seen
        except TypeError as exc:
            raise TypeError("uniqueness key must be hashable") from exc

        if already_seen:
            raise ValueError(f"duplicate items detected for key {uniqueness_key!r}")
        seen.add(uniqueness_key)

    return materialised


def non_empty(
    obj: Iterable[T] | Mapping[str, Any] | str,
    *,
    trim: bool = True,
) -> None:
    """Assert that *obj* is not empty.

    Parameters
    ----------
    obj
        Container to validate.
    trim
        When *obj* is a string, optionally strip whitespace before checking.

    Raises
    ------
    ValueError
        If the container is empty.
    TypeError
        If emptiness cannot be determined without consuming the iterable.

    Examples
    --------
    >>> non_empty([1, 2, 3])
    >>> non_empty(" value ")
    >>> non_empty(())
    Traceback (most recent call last):
        ...
    ValueError: container is empty

    Complexity
    ----------
    ``O(1)`` time when ``len`` is supported; otherwise raises ``TypeError``.
    """

    if isinstance(obj, str):
        candidate = obj.strip() if trim else obj
        if not candidate:
            raise ValueError("string is empty")
        return

    if isinstance(obj, Mapping):
        if not obj:
            raise ValueError("mapping is empty")
        return

    if not isinstance(obj, Iterable):
        raise TypeError("object is not iterable")

    if hasattr(obj, "__len__"):
        try:
            length = len(obj)  # type: ignore[arg-type]
        except TypeError as exc:  # pragma: no cover - defensive
            raise TypeError("unable to determine container length") from exc
        if length == 0:
            raise ValueError("container is empty")
        return

    raise TypeError(
        "cannot determine emptiness without consuming the iterable; materialise it first",
    )


def sort_normalized(
    items: Iterable[T], *, key: Callable[[T], Any] | None = None
) -> list[T]:
    """Return a deterministically sorted list of *items*.

    Examples
    --------
    >>> sort_normalized([3, 1, 2])
    [1, 2, 3]

    Complexity
    ----------
    ``O(n log n)`` time, ``O(n)`` memory.
    """

    materialised = list(ensure_iterable(items))
    return sorted(materialised, key=key)


def chunked(items: Iterable[T], size: int) -> Iterator[list[T]]:
    """Yield lists containing at most *size* elements from *items*.

    Parameters
    ----------
    items
        Source iterable.
    size
        Chunk size; must be greater than zero.

    Yields
    ------
    list
        Consecutive chunks preserving original order.

    Examples
    --------
    >>> list(chunked([1, 2, 3, 4], 2))
    [[1, 2], [3, 4]]

    Complexity
    ----------
    ``O(n)`` time, ``O(size)`` memory per chunk.
    """

    if size <= 0:
        raise ValueError("chunk size must be a positive integer")

    iterator = iter(ensure_iterable(items))
    while True:
        chunk: list[T] = []
        try:
            for _ in range(size):
                chunk.append(next(iterator))
        except StopIteration:
            if chunk:
                yield chunk
            break
        else:
            yield chunk


def require_keys(obj: Mapping[str, Any], keys: Sequence[str]) -> None:
    """Ensure that *obj* exposes all *keys*.

    Raises
    ------
    TypeError
        If *obj* is not a mapping.
    ValueError
        When at least one key is missing.

    Examples
    --------
    >>> require_keys({"id": 1, "name": "Ligand"}, ["id"])
    >>> require_keys({}, ["id"])
    Traceback (most recent call last):
        ...
    ValueError: missing required keys: {'id'}

    Complexity
    ----------
    ``O(k)`` time, where ``k`` is the number of required keys.
    """

    if not isinstance(obj, Mapping):
        raise TypeError("object is not a mapping")

    missing = {key for key in keys if key not in obj}
    if missing:
        raise ValueError(f"missing required keys: {missing}")


@overload
def coerce_to_int(obj: int) -> int: ...


@overload
def coerce_to_int(obj: str | float | Decimal) -> int: ...


def coerce_to_int(obj: Any) -> int:
    """Convert *obj* to ``int`` with validation.

    Raises
    ------
    ValueError
        If the value cannot be interpreted as an integer.
    TypeError
        For unsupported types or boolean inputs.

    Examples
    --------
    >>> coerce_to_int("42")
    42
    >>> coerce_to_int(3.0)
    3
    >>> coerce_to_int(True)
    Traceback (most recent call last):
        ...
    TypeError: booleans are not accepted

    Complexity
    ----------
    ``O(1)`` time.
    """

    if isinstance(obj, bool):
        raise TypeError("booleans are not accepted")

    if isinstance(obj, int):
        return obj

    if isinstance(obj, Decimal):
        if obj != obj.to_integral_value():
            raise ValueError("decimal value is not an integer")
        return int(obj)

    if isinstance(obj, float):
        if not obj.is_integer():
            raise ValueError("float value is not an integer")
        return int(obj)

    if isinstance(obj, str):
        text = obj.strip()
        if not text:
            raise ValueError("empty string is not an integer")
        try:
            return int(text, 10)
        except ValueError as exc:
            raise ValueError("invalid integer literal") from exc

    raise TypeError(f"cannot coerce {type(obj).__name__} to int")


@overload
def coerce_to_float(obj: float | int | Decimal) -> float: ...


@overload
def coerce_to_float(obj: str) -> float: ...


def coerce_to_float(obj: Any) -> float:
    """Convert *obj* to ``float`` with validation.

    Raises
    ------
    ValueError
        If the value cannot be interpreted as a finite float.
    TypeError
        For unsupported types or boolean inputs.

    Examples
    --------
    >>> coerce_to_float("0.5")
    0.5
    >>> coerce_to_float(2)
    2.0

    Complexity
    ----------
    ``O(1)`` time.
    """

    if isinstance(obj, bool):
        raise TypeError("booleans are not accepted")

    if isinstance(obj, float):
        if obj != obj:
            raise ValueError("float value is NaN")
        if obj in {float("inf"), float("-inf")}:
            raise ValueError("float value is infinite")
        return obj

    if isinstance(obj, int):
        return float(obj)

    if isinstance(obj, Decimal):
        value = float(obj)
        if value != value:
            raise ValueError("decimal value converted to NaN")
        if value in {float("inf"), float("-inf")}:
            raise ValueError("decimal value converted to infinity")
        return value

    if isinstance(obj, str):
        text = obj.strip()
        if not text:
            raise ValueError("empty string is not a float")
        try:
            value = float(text)
        except ValueError as exc:
            raise ValueError("invalid float literal") from exc
        if value != value:
            raise ValueError("string converted to NaN")
        if value in {float("inf"), float("-inf")}:
            raise ValueError("string converted to infinity")
        return value

    raise TypeError(f"cannot coerce {type(obj).__name__} to float")


def coerce_to_str(
    obj: Any,
    *,
    strip: bool = True,
    normalize_unicode: bool = True,
) -> str:
    """Convert *obj* to ``str`` with optional trimming and unicode normalisation.

    Parameters
    ----------
    obj
        Value to convert.
    strip
        Whether to strip leading/trailing whitespace.
    normalize_unicode
        Apply NFC normalisation when ``True``.

    Examples
    --------
    >>> coerce_to_str(b" ligand \n")
    'ligand'

    Complexity
    ----------
    ``O(n)`` in the length of the resulting string.
    """

    if obj is None:
        raise TypeError("None cannot be coerced to str")

    if isinstance(obj, bytes):
        text = obj.decode("utf-8")
    else:
        text = str(obj)

    if strip:
        text = text.strip()

    if normalize_unicode:
        text = unicodedata.normalize("NFC", text)

    return text


def normalize_date(value: str) -> str:
    """Validate and canonicalise RFC 3339 timestamps.

    Parameters
    ----------
    value
        Candidate timestamp string.

    Returns
    -------
    str
        Normalised timestamp string.

    Raises
    ------
    ValueError
        If *value* is not a valid RFC 3339 timestamp.

    Examples
    --------
    >>> normalize_date("2020-01-02T03:04:05+00:00")
    '2020-01-02T03:04:05Z'

    Complexity
    ----------
    ``O(1)`` time.
    """

    text = coerce_to_str(value)
    if not text:
        raise ValueError("timestamp cannot be empty")

    candidate = text[:-6] + "Z" if text.endswith("+00:00") else text
    candidate = candidate[:-1] + "Z" if candidate.endswith("z") else candidate

    if not validate_rfc3339(candidate):
        raise ValueError("timestamp is not RFC 3339 compliant")

    return candidate


def validate_enum(value: str, allowed: Sequence[str]) -> str:
    """Ensure that *value* belongs to the *allowed* collection.

    Raises
    ------
    ValueError
        If *value* is not part of *allowed*.

    Examples
    --------
    >>> validate_enum("A", ["A", "B"])
    'A'

    Complexity
    ----------
    ``O(n)`` time, ``O(1)`` memory.
    """

    if value not in allowed:
        raise ValueError(f"{value!r} is not a valid option")
    return value


def validate_url(value: str, *, allow_reference: bool = False) -> str:
    """Validate and canonicalise URLs against RFC 3986.

    Parameters
    ----------
    value
        Candidate URL.
    allow_reference
        When ``True`` accept URI references (relative URLs).

    Returns
    -------
    str
        Canonicalised URL.

    Raises
    ------
    ValueError
        If validation fails.

    Examples
    --------
    >>> validate_url("HTTPS://Example.com/path")
    'https://example.com/path'

    Complexity
    ----------
    ``O(n)`` time in the length of the URL.
    """

    text = coerce_to_str(value)
    if not text:
        raise ValueError("URL cannot be empty")

    rule = "URI_reference" if allow_reference else "URI"
    if not validate_rfc3986(text, rule=rule):
        raise ValueError("URL is not RFC 3986 compliant")

    parsed = urlparse(text)
    if not allow_reference and (not parsed.scheme or not parsed.netloc):
        raise ValueError("absolute URL must define scheme and host")

    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()
    path = _normalize_percent_encoded(parsed.path, safe="/-._~!$&'()*+,;=:@")
    params = _normalize_percent_encoded(parsed.params, safe="-._~!$&'()*+,;=:@")
    query = _normalize_percent_encoded(parsed.query, safe="/?-._~!$&'()*+,;=:@")
    fragment = _normalize_percent_encoded(parsed.fragment, safe="/?-._~!$&'()*+,;=:@")

    rebuilt = urlunparse((scheme, netloc, path, params, query, fragment))
    if not validate_rfc3986(rebuilt, rule=rule):  # defensive re-check
        raise ValueError("normalised URL is not RFC 3986 compliant")

    return rebuilt

# ChEMBL ID pattern
CHEMBL_ID_PATTERN = r"^CHEMBL\d+$"

# BAO ID pattern
BAO_ID_PATTERN = r"^BAO_\d{7}$"

# DOI pattern
DOI_PATTERN = r"^10\.\d{4,9}/\S+$"

# UUID pattern (lower/upper case supported)
UUID_PATTERN = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"


def _build_string_column(
    *,
    nullable: bool,
    unique: bool,
    checks: list[Check],
) -> Column:
    """Internal helper for constructing string columns."""
    return Column(
        pa.String,
        checks=checks or None,  # type: ignore[arg-type,assignment]
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
    checks: list[Check] = []
    if pattern is not None:
        checks.append(Check.str_matches(pattern))  # type: ignore[arg-type]
    return _build_string_column(nullable=nullable, unique=unique, checks=checks)


def object_column(*, nullable: bool) -> Column:
    """Create an object column with nullable control."""

    return Column(pa.Object, nullable=nullable)  # type: ignore[assignment]


def chembl_id_column(*, nullable: bool = True, unique: bool = False) -> Column:
    """Create a ChEMBL ID column with validation."""

    return string_column(nullable=nullable, unique=unique, pattern=CHEMBL_ID_PATTERN)


def nullable_string_column() -> Column:
    """Create a nullable string column.

    Returns
    -------
    Column
        A Pandera Column definition for nullable strings.
    """
    return string_column(nullable=True)


def non_nullable_string_column() -> Column:
    """Create a non-nullable string column.

    Returns
    -------
    Column
        A Pandera Column definition for non-nullable strings.
    """
    return string_column(nullable=False)


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
    base_column = string_column(nullable=nullable, pattern=pattern, unique=unique)
    initial_checks: list[Check] = list(base_column.checks or [])
    checks: list[Check] = list(initial_checks)
    if isin is not None:
        checks.append(Check.isin(isin))  # type: ignore[arg-type]
    if str_length is not None:
        checks.append(Check.str_length(str_length[0], str_length[1]))  # type: ignore[arg-type]

    if checks == initial_checks:
        return base_column
    return _build_string_column(nullable=nullable, unique=unique, checks=checks)


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


def bao_id_column(*, nullable: bool = True) -> Column:
    """Create a BAO ID column with validation.

    Parameters
    ----------
    nullable
        Whether the column can contain null values.

    Returns
    -------
    Column
        A Pandera Column definition for BAO IDs.
    """
    return string_column(nullable=nullable, pattern=BAO_ID_PATTERN)


def doi_column(*, nullable: bool = True) -> Column:
    """Create a DOI column with validation.

    Parameters
    ----------
    nullable
        Whether the column can contain null values.

    Returns
    -------
    Column
        A Pandera Column definition for DOIs.
    """
    return string_column(nullable=nullable, pattern=DOI_PATTERN)


def nullable_object_column() -> Column:
    """Create a nullable object column.

    Returns
    -------
    Column
        A Pandera Column definition for nullable objects.
    """
    return object_column(nullable=True)


def uuid_column(*, nullable: bool = False, unique: bool = False) -> Column:
    """Create a UUID column enforcing canonical hyphenated format."""

    return string_column(nullable=nullable, unique=unique, pattern=UUID_PATTERN)


__all__ = [
    "CHEMBL_ID_PATTERN",
    "BAO_ID_PATTERN",
    "DOI_PATTERN",
    "UUID_PATTERN",
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
