import json
from pathlib import Path

import pandas as pd
import pytest

from bioetl.utils.validation import ColumnComparisonResult, ColumnValidator


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

    def fake_compare_columns(self, entity, actual_df, schema_version="latest"):
        processed_entities.append(entity)
        return ColumnComparisonResult(
            entity=entity,
            expected_columns=[],
            actual_columns=list(actual_df.columns),
            missing_columns=[],
            extra_columns=[],
            order_matches=True,
            column_count_matches=True,
            empty_columns=[],
            non_empty_columns=list(actual_df.columns),
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
        "bioetl.schemas.registry.SchemaRegistry",
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
