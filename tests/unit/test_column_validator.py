import pytest

from bioetl.utils.column_validator import ColumnValidator


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
