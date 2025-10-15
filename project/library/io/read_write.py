"""IO helpers for the pipeline."""
from __future__ import annotations

from collections.abc import Iterable, Sequence
from pathlib import Path

import pandas as pd
from library.utils.errors import ValidationFailure


def read_input_csv(path: Path, schema) -> pd.DataFrame:  # type: ignore[no-untyped-def]
    """Read and validate the input CSV."""
    df = pd.read_csv(path)
    try:
        return schema.validate(df)
    except Exception as exc:  # noqa: BLE001 - pandera raises BaseException subclasses
        raise ValidationFailure(str(exc)) from exc


def write_output_csv(
    df: pd.DataFrame,
    path: Path,
    *,
    column_order: Sequence[str],
    sort_by: Iterable[str],
) -> None:
    """Write deterministic CSV output."""
    sorted_df = df.sort_values(list(sort_by), kind="mergesort")
    sorted_df = sorted_df.loc[:, list(column_order)]
    path.parent.mkdir(parents=True, exist_ok=True)
    sorted_df.to_csv(path, index=False, encoding="utf-8")
