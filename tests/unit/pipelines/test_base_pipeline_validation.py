"""Unit tests covering :meth:`bioetl.pipelines.base.PipelineBase.validate`."""

from __future__ import annotations

import pandas as pd
import pytest

from bioetl.config.loader import load_config
from bioetl.config.models import PipelineConfig
from bioetl.core.unified_schema import register_schema
from bioetl.pandera_pandas import DataFrameModel, Field
from bioetl.pandera_typing import Series
from bioetl.pipelines.base import PipelineBase


class _UnitTestSchema(DataFrameModel):
    value: Series[int] = Field(nullable=False)
    extra: Series[str] = Field(nullable=True)

    class Config:
        strict = True


register_schema(
    "unit_test_validation",
    "1.0.0",
    _UnitTestSchema,
    column_order=["value", "extra"],
    schema_id="unit_test.output",
)


class _DummyPipeline(PipelineBase):
    def extract(self, *args, **kwargs):  # pragma: no cover - unused in test
        raise NotImplementedError

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:  # pragma: no cover - unused in test
        return df

    def close_resources(self) -> None:  # pragma: no cover - no-op for tests
        return None


@pytest.fixture(name="unit_test_config")
def _unit_test_config() -> PipelineConfig:
    config = load_config("configs/pipelines/testitem.yaml")
    updated_pipeline = config.pipeline.model_copy(update={"entity": "unit_test_validation"})
    return config.model_copy(update={"pipeline": updated_pipeline})


def test_validate_reorders_columns_and_enforces_schema(unit_test_config: PipelineConfig) -> None:
    pipeline = _DummyPipeline(unit_test_config, run_id="unit-test")
    frame = pd.DataFrame({"extra": ["x"], "value": [1], "unexpected": ["keep"]})

    validated = pipeline.validate(frame)

    assert list(validated.columns) == ["value", "extra"]
    assert validated.loc[0, "value"] == 1
