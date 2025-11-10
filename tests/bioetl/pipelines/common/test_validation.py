from __future__ import annotations

import pandas as pd
import pandera.pandas as pa
import pytest
from pandera.errors import SchemaErrors

from bioetl.pipelines.common.validation import (
    format_failure_cases,
    summarize_schema_errors,
)


@pytest.fixture()
def schema_error() -> SchemaErrors:
    schema = pa.DataFrameSchema(
        {
            "value": pa.Column(pa.Int, checks=pa.Check.gt(0)),
            "name": pa.Column(pa.String, nullable=False),
        }
    )
    df = pd.DataFrame({"value": [1, 0, -1], "name": ["ok", None, ""]})

    with pytest.raises(SchemaErrors) as exc_info:
        schema.validate(df, lazy=True)

    return exc_info.value


@pytest.mark.unit
def test_summarize_schema_errors(schema_error: SchemaErrors) -> None:
    summary = summarize_schema_errors(schema_error)

    assert summary["affected_rows"] == 2
    assert summary["affected_columns"] == ["value", "name"]
    assert summary["error_types"] == {"Column": 3}
    assert summary["error_counts"] == {
        "DATAFRAME_CHECK": 1,
        "SERIES_CONTAINS_NULLS": 1,
    }
    assert summary["message"]


@pytest.mark.unit
def test_format_failure_cases(schema_error: SchemaErrors) -> None:
    failure_cases = schema_error.failure_cases
    assert isinstance(failure_cases, pd.DataFrame)

    formatted = format_failure_cases(failure_cases, sample_size=2)

    assert formatted["total_failures"] == 3
    assert formatted["unique_rows"] == 2
    assert formatted["error_types"] == {"Column": 3}
    assert formatted["column_errors"] == {"value": 2, "name": 1}
    assert len(formatted["sample"]) == 2
    assert formatted["sample"][0]["column"] == "value"


@pytest.mark.unit
def test_format_failure_cases_empty_frame_returns_empty_dict() -> None:
    empty_df = pd.DataFrame()

    assert format_failure_cases(empty_df) == {}
