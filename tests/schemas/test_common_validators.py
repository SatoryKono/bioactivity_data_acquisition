import math
from collections.abc import Iterator
from decimal import Decimal

import pytest

from bioetl.schemas.common import (
    chunked,
    coerce_to_float,
    coerce_to_int,
    coerce_to_str,
    ensure_iterable,
    ensure_unique,
    non_empty,
    normalize_date,
    require_keys,
    sort_normalized,
    validate_enum,
    validate_url,
)


def test_ensure_iterable_preserves_sequences() -> None:
    seq = [1, 2, 3]
    assert ensure_iterable(seq) is seq


def test_ensure_iterable_wraps_scalars() -> None:
    result = ensure_iterable(5)
    assert list(result) == [5]


@pytest.mark.parametrize(
    ("value", "allow_string", "expected"),
    [("abc", False, ["abc"]), ("abc", True, list("abc"))],
)
def test_ensure_iterable_string_handling(value: str, allow_string: bool, expected: list[str]) -> None:
    assert list(ensure_iterable(value, allow_string=allow_string)) == expected


def test_ensure_unique_success() -> None:
    assert ensure_unique(["a", "b"]) == ["a", "b"]


def test_ensure_unique_duplicate() -> None:
    with pytest.raises(ValueError):
        ensure_unique(["a", "a"])


def test_ensure_unique_requires_hashable_key() -> None:
    with pytest.raises(TypeError):
        ensure_unique([[1], [1]])


def test_non_empty_valid_cases() -> None:
    non_empty([1])
    non_empty({"k": "v"})
    non_empty(" value ")


@pytest.mark.parametrize("value", ["", "  ", [], {}])
def test_non_empty_rejects_empty_containers(value: object) -> None:
    with pytest.raises(ValueError):
        non_empty(value)  # type: ignore[arg-type]


def test_non_empty_iterator_requires_materialisation() -> None:
    def _generator() -> Iterator[int]:
        yield 1

    with pytest.raises(TypeError):
        non_empty(_generator())


def test_sort_normalized_returns_sorted_list() -> None:
    assert sort_normalized([3, 1, 2]) == [1, 2, 3]


def test_chunked_splits_iterable() -> None:
    assert list(chunked(range(5), 2)) == [[0, 1], [2, 3], [4]]


def test_chunked_requires_positive_size() -> None:
    with pytest.raises(ValueError):
        list(chunked([1, 2], 0))


def test_require_keys_success() -> None:
    require_keys({"a": 1, "b": 2}, ["a", "b"])


def test_require_keys_missing() -> None:
    with pytest.raises(ValueError):
        require_keys({"a": 1}, ["a", "b"])


def test_require_keys_type_error() -> None:
    with pytest.raises(TypeError):
        require_keys(42, ["a"])  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "value",
    ["42", "  10  ", 3, Decimal("7"), 5.0],
)
def test_coerce_to_int_success(value: object) -> None:
    assert coerce_to_int(value) == int(value)  # type: ignore[arg-type]


@pytest.mark.parametrize("value", ["", "abc", 3.5, Decimal("1.2")])
def test_coerce_to_int_rejects_invalid(value: object) -> None:
    with pytest.raises(ValueError):
        coerce_to_int(value)


def test_coerce_to_int_rejects_bool() -> None:
    with pytest.raises(TypeError):
        coerce_to_int(True)


@pytest.mark.parametrize(
    "value",
    ["0.5", "  1.0  ", 2, Decimal("3.14"), 5.0],
)
def test_coerce_to_float_success(value: object) -> None:
    assert math.isclose(coerce_to_float(value), float(value))  # type: ignore[arg-type]


@pytest.mark.parametrize("value", ["", "abc", float("nan"), float("inf"), Decimal("NaN")])
def test_coerce_to_float_rejects_invalid(value: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        coerce_to_float(value)


def test_coerce_to_float_rejects_bool() -> None:
    with pytest.raises(TypeError):
        coerce_to_float(False)


def test_coerce_to_str_decodes_bytes_and_normalises() -> None:
    assert coerce_to_str(" café ".encode()) == "café"


def test_coerce_to_str_rejects_none() -> None:
    with pytest.raises(TypeError):
        coerce_to_str(None)


def test_normalize_date_success() -> None:
    assert normalize_date("2020-01-02T03:04:05+00:00") == "2020-01-02T03:04:05Z"


def test_normalize_date_rejects_invalid() -> None:
    with pytest.raises(ValueError):
        normalize_date("2020-01-02")


def test_validate_enum_accepts_known_value() -> None:
    assert validate_enum("A", ["A", "B"]) == "A"


def test_validate_enum_rejects_unknown_value() -> None:
    with pytest.raises(ValueError):
        validate_enum("C", ["A", "B"])


def test_validate_url_canonicalises_absolute_uri() -> None:
    assert (
        validate_url("HTTPS://Example.com/%7Echembl")
        == "https://example.com/~chembl"
    )


def test_validate_url_allows_reference_when_requested() -> None:
    assert validate_url("/relative/path", allow_reference=True) == "/relative/path"


def test_validate_url_rejects_reference_by_default() -> None:
    with pytest.raises(ValueError):
        validate_url("/relative/path")
