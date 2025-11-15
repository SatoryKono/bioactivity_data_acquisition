from __future__ import annotations

import pytest

from bioetl.core.schema.column_factory import SchemaColumnFactory


def _check_pattern(column, expected_pattern: str) -> None:
    checks = list(column.checks or [])
    assert len(checks) == 1
    check = checks[0]
    assert check.statistics["pattern"] == expected_pattern


def test_string_id_helper_wraps_string_factory() -> None:
    column = SchemaColumnFactory._string_id(r"^TEST\d+$", nullable=False, unique=True)

    assert column.nullable is False
    assert column.unique is True
    _check_pattern(column, r"^TEST\d+$")


@pytest.mark.parametrize(
    ("factory", "kwargs", "pattern", "nullable", "unique"),
    [
        (SchemaColumnFactory.chembl_id, {}, SchemaColumnFactory.CHEMBL_ID_PATTERN, True, False),
        (SchemaColumnFactory.chembl_id, {"nullable": False, "unique": True}, SchemaColumnFactory.CHEMBL_ID_PATTERN, False, True),
        (SchemaColumnFactory.bao_id, {}, SchemaColumnFactory.BAO_ID_PATTERN, True, False),
        (SchemaColumnFactory.doi, {}, SchemaColumnFactory.DOI_PATTERN, True, False),
        (SchemaColumnFactory.uuid, {}, SchemaColumnFactory.UUID_PATTERN, False, False),
        (SchemaColumnFactory.uuid, {"nullable": True, "unique": True}, SchemaColumnFactory.UUID_PATTERN, True, True),
    ],
)
def test_identifier_columns_delegate_to_string_helper(
    factory, kwargs, pattern: str, nullable: bool, unique: bool
) -> None:
    column = factory(**kwargs)

    assert column.nullable is nullable
    assert column.unique is unique
    _check_pattern(column, pattern)
