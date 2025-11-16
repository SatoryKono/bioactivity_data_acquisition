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
        (SchemaColumnFactory.chembl_id, {}, SchemaColumnFactory.ID_PATTERNS["chembl_id"], True, False),
        (
            SchemaColumnFactory.chembl_id,
            {"nullable": False, "unique": True},
            SchemaColumnFactory.ID_PATTERNS["chembl_id"],
            False,
            True,
        ),
        (SchemaColumnFactory.bao_id, {}, SchemaColumnFactory.ID_PATTERNS["bao_id"], True, False),
        (SchemaColumnFactory.doi, {}, SchemaColumnFactory.ID_PATTERNS["doi"], True, False),
        (SchemaColumnFactory.uuid, {}, SchemaColumnFactory.ID_PATTERNS["uuid"], False, False),
        (
            SchemaColumnFactory.uuid,
            {"nullable": True, "unique": True},
            SchemaColumnFactory.ID_PATTERNS["uuid"],
            True,
            True,
        ),
    ],
)
def test_identifier_columns_delegate_to_string_helper(
    factory, kwargs, pattern: str, nullable: bool, unique: bool
) -> None:
    column = factory(**kwargs)

    assert column.nullable is nullable
    assert column.unique is unique
    _check_pattern(column, pattern)


def test_generic_identifier_factory() -> None:
    column = SchemaColumnFactory.identifier("chembl_id", nullable=False, unique=True)

    assert column.nullable is False
    assert column.unique is True
    _check_pattern(column, SchemaColumnFactory.ID_PATTERNS["chembl_id"])


def test_generic_identifier_factory_unknown_name() -> None:
    with pytest.raises(KeyError):
        SchemaColumnFactory.identifier("missing")
