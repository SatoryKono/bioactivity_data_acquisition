from __future__ import annotations

import pandas as pd
import pytest

from bioetl.core.schema.normalizers import IdentifierStats, StringStats
from bioetl.core.utils.mixins import CollectionFlagMixin
from bioetl.pipelines.qc.boundary_check import QCBoundaryReport, QCBoundaryViolation


@pytest.mark.parametrize(
    ("collection", "expected"),
    [
        (None, False),
        ([], False),
        ([1], True),
        ((), False),
        ((1,), True),
        ({}, False),
        ({"a": 1}, True),
        (set(), False),
        ({"x", "y"}, True),
        (range(0), False),
        (range(1), True),
        ("", False),
        ("data", True),
        (b"", False),
        (b"1", True),
        (bytearray(), False),
        (bytearray(b"xy"), True),
    ],
)
def test_has_items_primitives(collection: object, expected: bool) -> None:
    assert CollectionFlagMixin.has_items(collection) is expected


def test_has_items_pandas_objects() -> None:
    df = pd.DataFrame({"col": []})
    assert not CollectionFlagMixin.has_items(df)

    df_non_empty = pd.DataFrame({"col": [1]})
    assert CollectionFlagMixin.has_items(df_non_empty)

    series = pd.Series([], dtype=int)
    assert not CollectionFlagMixin.has_items(series)

    series_non_empty = pd.Series([1, 2, 3])
    assert CollectionFlagMixin.has_items(series_non_empty)

    index = pd.Index([])
    assert not CollectionFlagMixin.has_items(index)

    index_non_empty = pd.Index(["a"])
    assert CollectionFlagMixin.has_items(index_non_empty)


def test_identifier_stats_uses_collection_flag_mixin() -> None:
    stats = IdentifierStats()
    assert not stats.has_changes

    stats.add("col", 1, 0)
    assert stats.has_changes


def test_string_stats_uses_collection_flag_mixin() -> None:
    stats = StringStats()
    assert not stats.has_changes

    stats.add("col", 2)
    assert stats.has_changes


def test_qc_boundary_report_uses_collection_flag_mixin(
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    report = QCBoundaryReport(package="pkg", violations=())
    assert not report.has_violations

    source_dir = tmp_path_factory.mktemp("src")
    violation = QCBoundaryViolation(
        module="cli.module",
        qc_module="qc.module",
        import_chain=("cli.module", "qc.module"),
        source_path=source_dir / "module.py",
    )
    report_with_violation = QCBoundaryReport(package="pkg", violations=(violation,))
    assert report_with_violation.has_violations
