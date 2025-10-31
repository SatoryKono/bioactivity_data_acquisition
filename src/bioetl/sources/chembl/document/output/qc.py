"""QC summary helpers for the document pipeline."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Callable


AddSectionCallable = Callable[[str, Mapping[str, Any]], Any]


def append_qc_sections(
    add_section: AddSectionCallable,
    *,
    dataset_name: str,
    row_count: int,
    duplicates: Mapping[str, Any] | None = None,
    coverage: Mapping[str, float] | None = None,
) -> None:
    """Populate common QC sections for the provided dataset."""

    add_section("row_counts", {dataset_name: row_count})
    add_section("datasets", {dataset_name: {"rows": row_count}})
    if duplicates is not None:
        add_section("duplicates", {dataset_name: duplicates})
    if coverage is not None:
        add_section("coverage", {dataset_name: coverage})
