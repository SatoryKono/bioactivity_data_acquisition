"""Regression coverage for the validation chain orchestration."""

from __future__ import annotations

from collections.abc import Sequence

import pandas as pd
import pandera.errors
import pytest

from bioetl.config.models.models import PipelineConfig
from bioetl.pipelines.base import PipelineBase
from pandera import DataFrameSchema


class _ValidationProbePipeline(PipelineBase):
    """Minimal pipeline exposing the shared validation routine."""

    def extract_all(self) -> pd.DataFrame:  # pragma: no cover - unused in tests
        return pd.DataFrame()

    def extract_by_ids(self, ids: Sequence[str]) -> pd.DataFrame:  # pragma: no cover - unused
        return pd.DataFrame({"id": list(ids)})

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:  # pragma: no cover - unused
        return df


def _build_pipeline(config: PipelineConfig, run_id: str) -> _ValidationProbePipeline:
    return _ValidationProbePipeline(config=config, run_id=run_id)


def _configure_schema(config: PipelineConfig) -> None:
    config.validation.schema_out = "tests.support.simple_schema.SimpleSchema"
    config.validation.strict = True
    config.validation.coerce = True


def test_validation_chain_strict_mode_raises(
    pipeline_config_fixture: PipelineConfig,
    run_id: str,
) -> None:
    config = pipeline_config_fixture.model_copy(deep=True)
    _configure_schema(config)
    pipeline = _build_pipeline(config, run_id)

    df = pd.DataFrame({"id": [1], "value": ["invalid"]})

    with pytest.raises(pandera.errors.SchemaErrors):
        pipeline.validate(df)


def test_validation_chain_fail_open_records_summary(
    pipeline_config_fixture: PipelineConfig,
    run_id: str,
) -> None:
    config = pipeline_config_fixture.model_copy(deep=True)
    _configure_schema(config)
    config.cli.fail_on_schema_drift = False  # type: ignore[attr-defined]
    pipeline = _build_pipeline(config, run_id)

    df = pd.DataFrame({"id": [1], "value": ["invalid"]})

    validated = pipeline.validate(df)

    assert "hash_row" in validated.columns
    assert pipeline._validation_schema is not None
    summary = pipeline._validation_summary
    assert summary is not None
    assert summary["schema_valid"] is False
    assert summary["failure_count"] >= 1
    assert summary["schema_identifier"] == "tests.support.simple_schema.SimpleSchema"
    assert summary["error"]


def test_validation_chain_retries_without_coerce(
    pipeline_config_fixture: PipelineConfig,
    run_id: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = pipeline_config_fixture.model_copy(deep=True)
    _configure_schema(config)
    pipeline = _build_pipeline(config, run_id)

    original_validate = DataFrameSchema.validate
    call_sequence: list[bool] = []

    def _validate_with_coerce_failure(
        self: DataFrameSchema,
        df: pd.DataFrame,
        *args: object,
        **kwargs: object,
    ) -> pd.DataFrame:
        call_sequence.append(bool(self.coerce))
        if self.name == "SimpleSchema" and bool(self.coerce):
            failure_cases = pd.DataFrame(
                {
                    "schema_context": ["Column"],
                    "column": ["value"],
                    "check": ["coerce_dtype('float64')"],
                    "check_number": [None],
                    "failure_case": ["invalid"],
                    "index": [0],
                }
            )
            schema_error = pandera.errors.SchemaError(
                schema=self,
                data=df,
                message="coerce dtype failed",
                failure_cases=failure_cases,
                check="coerce_dtype('float64')",
                column_name="value",
                reason_code=pandera.errors.SchemaErrorReason.DATATYPE_COERCION,
            )
            raise pandera.errors.SchemaErrors(self, [schema_error], df)
        return original_validate(self, df, *args, **kwargs)

    monkeypatch.setattr(DataFrameSchema, "validate", _validate_with_coerce_failure)

    df = pd.DataFrame({"id": [1], "value": ["invalid"]})

    validated = pipeline.validate(df)

    assert call_sequence == [True, False]
    assert validated.loc[0, "value"] == "invalid"
    assert "hash_row" in validated.columns
    summary = pipeline._validation_summary
    assert summary is not None
    assert summary["schema_valid"] is True
