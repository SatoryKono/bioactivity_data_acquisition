from __future__ import annotations

import pandas as pd
import pytest

from bioetl.core.pipeline.base import PipelineBase
from bioetl.core.pipeline.cli import run_cli
from bioetl.core.validation import (
    DEFAULT_RECORD_SCHEMA,
    DuplicateRowsRule,
    MissingRateRule,
    PanderaSchemaProvider,
    PanderaValidator,
)


class DummyPipeline(PipelineBase):
    def __init__(self, data, **kwargs):
        super().__init__(run_id="test", **kwargs)
        self._data = data

    def extract(self) -> pd.DataFrame:
        return pd.DataFrame(self._data)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:  # pragma: no cover - passthrough
        return df


@pytest.fixture()
def validator():
    return PanderaValidator(PanderaSchemaProvider(DEFAULT_RECORD_SCHEMA))


def test_validation_passes_with_strict_mode(validator):
    pipeline = DummyPipeline(
        data=[{"id": 1, "name": "mol", "value": 0.5}],
        validator=validator,
        dq_rules=[DuplicateRowsRule(subset=["id"]), MissingRateRule(columns=["value"], max_missing_rate=0.5)],
        strict_validation=True,
    )

    result = pipeline.run()

    assert result.total_rows_read == 1


def test_validation_fails_on_schema_error(validator):
    pipeline = DummyPipeline(
        data=[{"id": -1, "name": "", "value": None}],
        validator=validator,
        dq_rules=[DuplicateRowsRule(subset=["id"]), MissingRateRule(columns=["value"], max_missing_rate=0.0)],
        strict_validation=False,
    )

    with pytest.raises(ValueError):
        pipeline.run()


def test_cli_strict_validation_flag_enforced(monkeypatch, validator):
    called = {"strict": None}

    def factory(strict_validation: bool) -> PipelineBase:
        called["strict"] = strict_validation
        return DummyPipeline(
            data=[{"id": 1, "name": "mol", "value": None}],
            validator=validator,
            dq_rules=[MissingRateRule(columns=["value"], max_missing_rate=0.0, severity="warning")],
            strict_validation=strict_validation,
        )

    with pytest.raises(SystemExit) as excinfo:
        run_cli(factory, ["--strict-validation"])

    assert excinfo.value.code == 1
    assert called["strict"] is True
