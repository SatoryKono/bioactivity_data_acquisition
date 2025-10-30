import io
import json
from pathlib import Path

import pandas as pd
import pytest

from bioetl.utils.validation import (
    ColumnComparisonResult,
    ColumnValidator,
    SchemaRegistry,
)


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


@pytest.mark.parametrize(
    "artifact_name",
    [
        "assay_qc_summary.json",
        "targets_qc_missing_mappings.csv",
        "targets_qc_enrichment_metrics.csv",
        "assay_summary_statistics.csv",
        "target_dataset_metrics.csv",
    ],
)
def test_should_skip_qc_artifacts(artifact_name: str) -> None:
    validator = ColumnValidator()

    assert validator._should_skip_file(Path(artifact_name))


def test_validate_pipeline_output_processes_files_in_sorted_order(
    tmp_path, monkeypatch
) -> None:
    validator = ColumnValidator()
    processed_entities: list[str] = []

    def fake_compare_columns(
        self,
        entity,
        actual_columns,
        schema_version="latest",
        *,
        empty_columns=None,
        non_empty_columns=None,
    ):
        processed_entities.append(entity)
        return ColumnComparisonResult(
            entity=entity,
            expected_columns=[],
            actual_columns=list(actual_columns),
            missing_columns=[],
            extra_columns=[],
            order_matches=True,
            column_count_matches=True,
            empty_columns=list(empty_columns or []),
            non_empty_columns=list(non_empty_columns or actual_columns),
            duplicate_columns={},
        )

    monkeypatch.setattr(ColumnValidator, "compare_columns", fake_compare_columns)

    file_names = [
        "zeta_pipeline.csv",
        "alpha_pipeline.csv",
        "beta_pipeline.csv",
    ]

    for name in file_names:
        (tmp_path / name).write_text("column\nvalue\n", encoding="utf-8")

    results = validator.validate_pipeline_output("pipeline", tmp_path)

    expected_entities = [
        validator._extract_entity_from_filename(name) for name in sorted(file_names)
    ]

    assert processed_entities == expected_entities
    assert [result.entity for result in results] == expected_entities
def test_compare_columns_detects_duplicate_columns(monkeypatch: pytest.MonkeyPatch) -> None:
    validator = ColumnValidator()

    monkeypatch.setattr(
        SchemaRegistry,
        "get",
        lambda *args, **kwargs: object(),
    )
    monkeypatch.setattr(
        ColumnValidator,
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


def test_validate_pipeline_output_reads_header_and_chunks(
    tmp_path, monkeypatch
) -> None:
    validator = ColumnValidator()

    csv_path = tmp_path / "assay_output.csv"
    csv_path.write_text("col_a,col_b\n1,2\n3,4\n", encoding="utf-8")

    monkeypatch.setattr(
        SchemaRegistry,
        "get",
        lambda *args, **kwargs: object(),
    )
    monkeypatch.setattr(
        ColumnValidator,
        "_get_expected_columns",
        lambda self, schema: ["col_a", "col_b"],
    )

    read_kwargs: list[dict[str, object]] = []
    real_read_csv = pd.read_csv

    def tracking_read_csv(*args, **kwargs):
        read_kwargs.append(dict(kwargs))
        return real_read_csv(*args, **kwargs)

    monkeypatch.setattr(pd, "read_csv", tracking_read_csv)

    results = validator.validate_pipeline_output("assay", tmp_path)

    assert results, "Ожидался хотя бы один результат валидации"
    assert results[0].non_empty_columns == ["col_a", "col_b"]
    assert any(kwargs.get("nrows") == 0 for kwargs in read_kwargs)
    assert any("chunksize" in kwargs for kwargs in read_kwargs)


def test_analyze_file_column_data_handles_large_stringio() -> None:
    validator = ColumnValidator()

    rows = 2048
    lines = ["col_a,col_b,col_empty"]
    for i in range(rows):
        col_a = i
        col_b = i if i % 5 else ""
        lines.append(f"{col_a},{col_b},")

    buffer = io.StringIO("\n".join(lines))

    empty_columns, non_empty_columns = validator._analyze_file_column_data(
        buffer,
        ["col_a", "col_b", "col_empty"],
        chunk_size=256,
    )

    assert empty_columns == ["col_empty"]
    assert non_empty_columns == ["col_a", "col_b"]
