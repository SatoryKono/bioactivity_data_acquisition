"""I/O helpers for interacting with CSV artifacts."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from library.etl.transform import _resolve_ascending
from library.io_.normalize import (
    PUBLICATION_COLUMNS,
    normalize_publication_frame,
    normalize_query_frame,
)

if TYPE_CHECKING:  # pragma: no cover - import for typing only
    from library.config import (
        CsvFormatSettings,
        DeterminismSettings,
        OutputSettings,
        ParquetFormatSettings,
    )


def _csv_options(settings: CsvFormatSettings) -> dict[str, object]:
    options: dict[str, object] = {
        "index": False,
        "encoding": settings.encoding,
    }
    if settings.float_format is not None:
        options["float_format"] = settings.float_format
    if settings.date_format is not None:
        options["date_format"] = settings.date_format
    if settings.na_rep is not None:
        options["na_rep"] = settings.na_rep
    if settings.line_terminator is not None:
        options["line_terminator"] = settings.line_terminator
    return options


def read_queries(path: Path) -> pd.DataFrame:
    """Load a CSV containing search queries."""

    frame = pd.read_csv(path)
    return normalize_query_frame(frame)


def write_publications(
    df: pd.DataFrame,
    path: Path,
    *,
    determinism: DeterminismSettings | None = None,
    output: OutputSettings | None = None,
) -> None:
    """Persist publication records using deterministic configuration."""

    from library.config import (
        CsvFormatSettings as _CsvFormatSettings,
    )
    from library.config import (
        ParquetFormatSettings as _ParquetFormatSettings,
    )

    normalized = normalize_publication_frame(df)

    csv_settings: CsvFormatSettings
    parquet_settings: ParquetFormatSettings
    file_format = "csv"
    if output is not None:
        csv_settings = output.csv
        parquet_settings = output.parquet
        file_format = output.format
    else:  # pragma: no cover - fallback
        csv_settings = _CsvFormatSettings()
        parquet_settings = _ParquetFormatSettings()

    if determinism is not None:
        column_order = [col for col in determinism.column_order if col in normalized.columns]
        remaining = [col for col in normalized.columns if col not in column_order]
        ordered_columns = column_order + remaining
        if ordered_columns:
            normalized = normalized[ordered_columns]

        sort_by = determinism.sort.by or ordered_columns or normalized.columns.tolist()
        ascending = _resolve_ascending(sort_by, determinism.sort.ascending)
        normalized = normalized.sort_values(
            sort_by,
            ascending=ascending,
            na_position=determinism.sort.na_position,
        ).reset_index(drop=True)
    else:
        normalized = normalized.sort_values(sorted(normalized.columns)).reset_index(drop=True)

    path.parent.mkdir(parents=True, exist_ok=True)
    if file_format == "parquet":
        normalized.to_parquet(path, index=False, compression=parquet_settings.compression)
    else:
        normalized.to_csv(path, **_csv_options(csv_settings))


def empty_publications_frame() -> pd.DataFrame:
    """Return an empty DataFrame with the canonical publication columns."""

    return pd.DataFrame(columns=PUBLICATION_COLUMNS)
