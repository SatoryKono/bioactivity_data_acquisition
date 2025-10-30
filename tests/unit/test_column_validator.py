import io
import json
from pathlib import Path
from typing import Any
import pandas as pd
import pytest

import bioetl.utils.column_validator as column_validator_module
from bioetl.utils.column_validator import ColumnComparisonResult, ColumnValidator


@pytest.mark.parametrize(
    "filename, expected_entity",
    [
        ("assay_20240101T120000.csv", "assay"),
        ("Activity_2023-11-12.csv", "activity"),
        ("testitems_20231212_101010.csv", "testitem"),
        ("targets_export_20240101.csv", "target"),
        ("documents.csv", "document"),
        ("custom_entity.csv", "custom"),
    ],
)
def test_extract_entity_from_filename_handles_timestamps(filename: str, expected_entity: str) -> None:
    validator = ColumnValidator()

    extracted = validator._extract_entity_from_filename(filename)

    assert extracted == expected_entity


def test_generate_report_uses_utc_timestamp(tmp_path) -> None:
    validator = ColumnValidator()
    result = ColumnComparisonResult(
        entity="assay",
        expected_columns=["col_a", "col_b"],
        actual_columns=["col_a", "col_b"],
        missing_columns=[],
        extra_columns=[],
        order_matches=True,
        column_count_matches=True,
        empty_columns=[],
        non_empty_columns=["col_a", "col_b"],
        duplicate_columns={},
    )

    validator.generate_report([result], tmp_path)

    json_path = tmp_path / "column_comparison_report.json"
    report_data = json.loads(json_path.read_text(encoding="utf-8"))
    timestamp = report_data["timestamp"]

    assert timestamp.endswith("Z") or timestamp.endswith("+00:00")


def test_compare_columns_detects_duplicate_columns(monkeypatch: pytest.MonkeyPatch) -> None:
    validator = ColumnValidator()

    monkeypatch.setattr(
        column_validator_module.SchemaRegistry,
        "get",
        lambda *args, **kwargs: object(),
    )
    monkeypatch.setattr(
        column_validator_module.ColumnValidator,
        "_get_expected_columns",
        lambda self, schema: ["col_a", "col_b"],
    )

    df = pd.DataFrame([[1, 2, 3]], columns=["col_a", "col_a", "col_b"])

    result = validator.compare_columns("assay", df)

    assert result.duplicate_columns == {"col_a": 2}
    result_dict = result.to_dict()
    assert result_dict["duplicate_columns"] == {"col_a": 2}
    assert result_dict["duplicate_unique_count"] == 1
    assert result_dict["duplicate_total"] == 1


def test_analyze_csv_empty_columns_with_chunks() -> None:
    validator = ColumnValidator()

    rows = 5000
    csv_lines = ["col_a,col_b,col_empty"]
    for i in range(rows):
        csv_lines.append(f"{i},{i * 2},")
    csv_lines.append(",,")  # полностью пустая строка
    csv_buffer = io.StringIO("\n".join(csv_lines))

    columns = ["col_a", "col_b", "col_empty"]
    empty_columns, non_empty_columns = validator._analyze_csv_empty_columns(
        csv_buffer,
        columns,
        chunk_size=512,
    )

    assert empty_columns == ["col_empty"]
    assert set(non_empty_columns) == {"col_a", "col_b"}


def test_validate_pipeline_output_reads_header_first(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    validator = ColumnValidator()

    pipeline_dir = tmp_path / "assay"
    pipeline_dir.mkdir()
    csv_path = pipeline_dir / "assay_output.csv"

    rows = ["col_a,col_b,col_empty"]
    rows.extend(f"{i},{i * 10}," for i in range(10))
    csv_path.write_text("\n".join(rows), encoding="utf-8")

    monkeypatch.setattr(
        column_validator_module.SchemaRegistry,
        "get",
        lambda *args, **kwargs: object(),
    )
    monkeypatch.setattr(
        column_validator_module.ColumnValidator,
        "_get_expected_columns",
        lambda self, schema: ["col_a", "col_b", "col_empty"],
    )

    calls: list[dict[str, Any]] = []
    real_read_csv = column_validator_module.pd.read_csv

    def tracking_read_csv(*args, **kwargs):
        calls.append({"args": args, "kwargs": kwargs})
        return real_read_csv(*args, **kwargs)

    monkeypatch.setattr(column_validator_module.pd, "read_csv", tracking_read_csv)

    results = validator.validate_pipeline_output(
        pipeline_name="assay",
        output_dir=tmp_path,
        schema_version="latest",
        empty_column_chunk_size=4,
    )

    assert calls, "pd.read_csv should be called at least once"
    first_call_kwargs = calls[0]["kwargs"]
    assert first_call_kwargs.get("nrows") == 0

    assert len(results) == 1
    result = results[0]
    assert result.empty_columns == ["col_empty"]
    assert set(result.non_empty_columns) == {"col_a", "col_b"}
