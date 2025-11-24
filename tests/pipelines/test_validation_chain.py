"""Regression coverage for the validation chain orchestration."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pandas as pd
import pandera.errors
import pytest
from pandera import DataFrameSchema

from infrastructure.config.models.models import PipelineConfig
from application.pipelines.specs.base import PipelineBase


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

    # Patch backend validation to track call sequence and simulate retry success
    call_sequence: list[bool] = []

    # Import the actual backend class used internally by pandera
    from pandera.backends.pandas.container import DataFrameSchemaBackend

    original_backend_validate = DataFrameSchemaBackend.validate

    def patched_backend_validate(
        self: Any,
        check_obj: pd.DataFrame,
        schema: DataFrameSchema,
        *args: object,
        **kwargs: object,
    ) -> pd.DataFrame:
        schema_name = getattr(schema, "name", None)
        if schema_name == "SimpleSchema":
            # Track call sequence for test assertions
            coerce_value = bool(getattr(schema, "coerce", False))
            call_sequence.append(coerce_value)

            # If coerce=False, bypass validation and return data as-is
            # This simulates successful validation when only coercion errors occurred
            if not coerce_value:
                return check_obj.copy()

        # If coerce=True or not SimpleSchema, invoke original validation
        return original_backend_validate(self, check_obj, schema, *args, **kwargs)

    monkeypatch.setattr(DataFrameSchemaBackend, "validate", patched_backend_validate)

    df = pd.DataFrame({"id": [1], "value": ["invalid"]})

    validated = pipeline.validate(df)

    assert call_sequence == [True, False]
    assert validated.loc[0, "value"] == "invalid"
    assert "hash_row" in validated.columns
    summary = pipeline._validation_summary
    assert summary is not None
    assert summary["schema_valid"] is True
