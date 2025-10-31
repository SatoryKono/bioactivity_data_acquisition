"""Schema regression tests for the fallback metadata mixin."""

from __future__ import annotations

from collections.abc import Mapping

import pandas as pd
from hypothesis import given, strategies as st

from bioetl.schemas.base import (
    FALLBACK_METADATA_COLUMN_ORDER,
    BaseSchema,
    FallbackMetadataMixin,
)
from bioetl.pandera_pandas import Field
from bioetl.pandera_typing import Series


class _FallbackEnabledSchema(FallbackMetadataMixin, BaseSchema):
    """Minimal schema including the fallback metadata mixin."""

    measurement: Series[int] = Field(nullable=False, ge=0)
    _column_order = [
        "index",
        "hash_row",
        "hash_business_key",
        "pipeline_version",
        "run_id",
        "source_system",
        "chembl_release",
        "extracted_at",
        "measurement",
        *FALLBACK_METADATA_COLUMN_ORDER,
    ]


_fallback_value_strategy = st.fixed_dictionaries(
    {
        "fallback_reason": st.one_of(st.none(), st.text(min_size=1, max_size=20)),
        "fallback_error_type": st.one_of(st.none(), st.text(min_size=1, max_size=20)),
        "fallback_error_code": st.one_of(st.none(), st.text(min_size=1, max_size=20)),
        "fallback_error_message": st.one_of(st.none(), st.text(min_size=1, max_size=50)),
        "fallback_http_status": st.one_of(st.none(), st.integers(min_value=0, max_value=999)),
        "fallback_retry_after_sec": st.one_of(
            st.none(), st.floats(min_value=0.0, max_value=1e6, allow_nan=False, allow_infinity=False)
        ),
        "fallback_attempt": st.one_of(st.none(), st.integers(min_value=0, max_value=20)),
        "fallback_timestamp": st.one_of(st.none(), st.text(min_size=1, max_size=30)),
    }
)


@given(_fallback_value_strategy, st.integers(min_value=0, max_value=10))
def test_schema_accepts_nullable_fallback_columns(
    fallback_payload: Mapping[str, object | None], measurement: int
) -> None:
    """Pandera validation should succeed for optional fallback metadata fields."""

    base_row = {
        "index": 1,
        "hash_row": "a" * 64,
        "hash_business_key": "b" * 64,
        "pipeline_version": "1.0.0-test",
        "run_id": "contract-run",
        "source_system": "chembl",
        "chembl_release": "ChEMBL_33",
        "extracted_at": "2024-01-01T00:00:00+00:00",
        "measurement": int(measurement),
    }
    base_row.update(fallback_payload)

    frame = pd.DataFrame([base_row])
    validated = _FallbackEnabledSchema.validate(frame)

    expected_order = _FallbackEnabledSchema.get_column_order()
    assert expected_order
    assert validated.columns.tolist()[: len(expected_order)] == expected_order

    for column, value in fallback_payload.items():
        assert column in validated.columns
        cell = validated.iloc[0][column]
        if value is None:
            assert pd.isna(cell)
        else:
            assert cell == value


@given(st.lists(st.text(min_size=1, max_size=5), max_size=3))
def test_column_order_accessor_returns_copy(extra_tokens: list[str]) -> None:
    """Mutating the descriptor return value should not affect the schema order."""

    returned = _FallbackEnabledSchema.Config.column_order
    mutated = list(returned)
    mutated.extend(extra_tokens)
    mutated.append("sentinel")

    assert _FallbackEnabledSchema.Config.column_order == returned
    assert _FallbackEnabledSchema.get_column_order() == returned
