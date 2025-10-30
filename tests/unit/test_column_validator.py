import io
import json

import pytest

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
    )

    validator.generate_report([result], tmp_path)

    json_path = tmp_path / "column_comparison_report.json"
    report_data = json.loads(json_path.read_text(encoding="utf-8"))
    timestamp = report_data["timestamp"]

    assert timestamp.endswith("Z") or timestamp.endswith("+00:00")


def test_analyze_csv_column_data_handles_large_mock_file(monkeypatch: pytest.MonkeyPatch) -> None:
    validator = ColumnValidator()

    rows = ["col_a,col_b,col_c"]
    for index in range(128):
        rows.append(f"{index},,{index}")

    csv_payload = "\n".join(rows)
    buffer = io.StringIO(csv_payload)

    # Смещаем указатель в конец, чтобы убедиться в сбросе позиции
    buffer.seek(len(csv_payload))

    empty_columns, non_empty_columns = validator._analyze_csv_column_data(  # noqa: SLF001
        buffer,
        ["col_a", "col_b", "col_c"],
        chunksize=16,
    )

    assert empty_columns == ["col_b"]
    assert set(non_empty_columns) == {"col_a", "col_c"}


def test_validate_pipeline_output_reads_header_and_streams(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    validator = ColumnValidator()
    csv_path = tmp_path / "assay_export.csv"
    csv_path.write_text(
        "col_a,col_b,col_c\n1,,1\n2,2,\n", encoding="utf-8"
    )

    import bioetl.utils.column_validator as column_validator_module

    monkeypatch.setattr(
        column_validator_module.ColumnValidator,
        "_get_expected_columns",
        lambda self, schema: ["col_a", "col_b", "col_c"],
    )

    class DummySchema:
        ...

    def fake_get(cls, entity: str, schema_version: str = "latest") -> DummySchema:
        return DummySchema()

    monkeypatch.setattr(
        column_validator_module.SchemaRegistry,
        "get",
        classmethod(fake_get),
    )

    calls: list[dict[str, object]] = []
    original_read_csv = column_validator_module.pd.read_csv

    def spy_read_csv(*args, **kwargs):  # type: ignore[no-untyped-def]
        calls.append(dict(kwargs))
        return original_read_csv(*args, **kwargs)

    monkeypatch.setattr(column_validator_module.pd, "read_csv", spy_read_csv)

    results = validator.validate_pipeline_output("assay", tmp_path)

    assert calls, "pd.read_csv was not invoked"
    assert calls[0].get("nrows") == 0
    assert any("chunksize" in call and call["chunksize"] for call in calls)

    assert len(results) == 1
    comparison = results[0]
    assert comparison.actual_columns == ["col_a", "col_b", "col_c"]
    assert comparison.empty_columns == ["col_c"]
