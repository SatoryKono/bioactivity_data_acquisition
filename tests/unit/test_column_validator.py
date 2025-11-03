import io
import json
from pathlib import Path

import pandas as pd
import pytest

from bioetl.utils.validation import (
    _DEFAULT_VALIDATION_CHUNK_SIZE,
    ColumnComparisonResult,
    ColumnValidator,
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
        actual_df,
        schema_version="latest",
        **kwargs,
    ):
        processed_entities.append(entity)
        column_non_null_counts = kwargs.get("column_non_null_counts", [])
        if actual_df is not None:
            actual_columns = list(actual_df.columns)
        else:
            actual_columns = [name for name, _ in column_non_null_counts]
        return ColumnComparisonResult(
            entity=entity,
            expected_columns=[],
            actual_columns=actual_columns,
            missing_columns=[],
            extra_columns=[],
            order_matches=True,
            column_count_matches=True,
            empty_columns=[],
            non_empty_columns=actual_columns,
            duplicate_columns={},
            source_file=kwargs.get("source_file"),
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


def test_validate_pipeline_output_streams_large_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    validator = ColumnValidator()

    csv_lines = ["assay_id,value,empty_col"]
    for idx in range(10_000):
        csv_lines.append(f"{idx},{idx},")

    csv_text = "\n".join(csv_lines)
    csv_path = tmp_path / "assay_output.csv"
    csv_path.write_text(csv_text, encoding="utf-8")

    monkeypatch.setattr(
        "bioetl.utils.validation.SchemaRegistry.get",
        lambda *args, **kwargs: object(),
    )
    monkeypatch.setattr(
        ColumnValidator,
        "_get_expected_columns",
        lambda self, schema: ["assay_id", "value", "empty_col"],
    )

    original_read_csv = pd.read_csv
    read_calls: list[dict[str, object]] = []

    def tracking_read_csv(path_or_buf, *args, **kwargs):  # type: ignore[override]
        if path_or_buf == csv_path:
            read_calls.append(kwargs.copy())
            buffer = io.StringIO(csv_text)
            return original_read_csv(buffer, *args, **kwargs)
        return original_read_csv(path_or_buf, *args, **kwargs)

    monkeypatch.setattr(pd, "read_csv", tracking_read_csv)

    results = validator.validate_pipeline_output("assay", tmp_path)

    assert len(results) == 1
    result = results[0]
    assert result.empty_columns == ["empty_col"]
    assert result.non_empty_columns == ["assay_id", "value"]
    assert read_calls[0].get("nrows") == 0
    assert any(
        call.get("chunksize") == _DEFAULT_VALIDATION_CHUNK_SIZE for call in read_calls
    )


def test_calculate_non_null_counts_with_large_in_memory_csv(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    validator = ColumnValidator()
    csv_path = Path("in_memory.csv")

    total_rows = 50_000
    lines = ["assay_id,value,empty_col"]
    expected_non_null_value = 0
    for idx in range(total_rows):
        value = "" if idx % 5 == 0 else str(idx)
        if value:
            expected_non_null_value += 1
        lines.append(f"{idx},{value},")

    csv_text = "\n".join(lines)

    original_read_csv = pd.read_csv
    read_calls: list[dict[str, object]] = []

    def fake_read_csv(path_or_buf, *args, **kwargs):  # type: ignore[override]
        if path_or_buf == csv_path:
            read_calls.append(kwargs.copy())
            buffer = io.StringIO(csv_text)
            return original_read_csv(buffer, *args, **kwargs)
        return original_read_csv(path_or_buf, *args, **kwargs)

    monkeypatch.setattr(pd, "read_csv", fake_read_csv)

    counts = validator._calculate_non_null_counts(
        csv_path,
        ["assay_id", "value", "empty_col"],
        chunk_size=10_000,
    )

    assert counts == [
        ("assay_id", total_rows),
        ("value", expected_non_null_value),
        ("empty_col", 0),
    ]
    assert any(call.get("chunksize") == 10_000 for call in read_calls)
def test_compare_columns_detects_duplicate_columns(monkeypatch: pytest.MonkeyPatch) -> None:
    validator = ColumnValidator()

    monkeypatch.setattr(
        "bioetl.utils.validation.SchemaRegistry.get",
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


def test_assert_expected_layout_passes_for_matching_results(tmp_path: Path) -> None:
    validator = ColumnValidator()
    dataset = tmp_path / "assay_output.csv"
    dataset.write_text("col_a,col_b\n1,2\n", encoding="utf-8")

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
        source_file=dataset,
    )

    validator.assert_expected_layout([result])


def test_assert_expected_layout_raises_on_name_mismatch(tmp_path: Path) -> None:
    validator = ColumnValidator()
    dataset = tmp_path / "unexpected_output.csv"
    dataset.write_text("col\n", encoding="utf-8")

    result = ColumnComparisonResult(
        entity="assay",
        expected_columns=["col"],
        actual_columns=["col"],
        missing_columns=[],
        extra_columns=[],
        order_matches=True,
        column_count_matches=True,
        empty_columns=[],
        non_empty_columns=["col"],
        duplicate_columns={},
        source_file=dataset,
    )

    with pytest.raises(AssertionError) as exc_info:
        validator.assert_expected_layout([result])

    assert "unexpected_output.csv" in str(exc_info.value)


def test_assert_expected_layout_raises_on_column_mismatch(tmp_path: Path) -> None:
    validator = ColumnValidator()
    dataset = tmp_path / "assay_output.csv"
    dataset.write_text("col\n", encoding="utf-8")

    result = ColumnComparisonResult(
        entity="assay",
        expected_columns=["col_a"],
        actual_columns=["col"],
        missing_columns=["col_a"],
        extra_columns=["col"],
        order_matches=False,
        column_count_matches=False,
        empty_columns=[],
        non_empty_columns=["col"],
        duplicate_columns={},
        source_file=dataset,
    )

    with pytest.raises(AssertionError) as exc_info:
        validator.assert_expected_layout([result])

    assert "mismatch" in str(exc_info.value)
