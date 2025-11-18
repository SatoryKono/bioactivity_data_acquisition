from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd

from bioetl.core.logging import LogEvents
from bioetl.core.schema import IdentifierRule, StringRule
from bioetl.pipelines.mixins import (
    NestedColumnSpec,
    NestedSerializerMixin,
    RecordNormalizationMixin,
)


class _RecordPipelineStub(RecordNormalizationMixin):
    def __init__(self) -> None:
        self.preprocess_invocations = 0

    def identifier_rules(self) -> tuple[IdentifierRule, ...]:
        return (
            IdentifierRule(columns=["chembl_id"], pattern=r"^CHEMBL\d+$"),
        )

    def string_rules(self) -> dict[str, StringRule]:
        return {"title": StringRule(max_length=10)}

    def preprocess_string_columns(self, df: pd.DataFrame, log: MagicMock) -> pd.DataFrame:
        self.preprocess_invocations += 1
        return df


class _SerializerPipelineStub(NestedSerializerMixin):
    def nested_column_specs(self) -> tuple[NestedColumnSpec, ...]:
        return (
            NestedColumnSpec(column="payload"),
            NestedColumnSpec(
                column="custom",
                serializer=lambda value, _log=None: "::" + str(value),
                empty_string_to_null=True,
            ),
        )


def test_record_normalization_mixin_applies_rules() -> None:
    pipeline = _RecordPipelineStub()
    df = pd.DataFrame(
        {
            "chembl_id": ["chembl1", "invalid"],
            "title": ["  Example  ", ""],
        }
    )
    log = MagicMock()

    normalized_ids = pipeline._normalize_identifiers(df, log)
    normalized_strings = pipeline._normalize_string_fields(normalized_ids, log)

    assert normalized_ids["chembl_id"].tolist() == ["CHEMBL1", pd.NA]
    assert normalized_strings["title"].tolist() == ["Example", pd.NA]
    assert pipeline.preprocess_invocations == 1
    log.debug.assert_called_with(
        LogEvents.STRING_FIELDS_NORMALIZED,
        columns=["title"],
        rows_processed=1,
    )


def test_nested_serializer_mixin_serializes_columns() -> None:
    pipeline = _SerializerPipelineStub()
    df = pd.DataFrame(
        {
            "payload": [{"a": 1}],
            "custom": ["value"],
        }
    )
    log = MagicMock()

    result = pipeline._serialize_nested_columns(df, log)

    assert result.loc[0, "payload"] == '{"a": 1}'
    assert result.loc[0, "custom"] == "::value"
    log.debug.assert_called_with(LogEvents.ARRAY_FIELDS_SERIALIZED, columns=["payload", "custom"])
