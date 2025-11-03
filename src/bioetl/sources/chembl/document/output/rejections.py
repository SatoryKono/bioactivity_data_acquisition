"""Helpers for persisting rejected input identifiers."""

from __future__ import annotations

from pathlib import Path
from typing import Callable, Iterable

import pandas as pd

from bioetl.core.logger import UnifiedLogger

logger = UnifiedLogger.get(__name__)


def persist_rejected_inputs(
    rows: Iterable[dict[str, str]],
    *,
    add_table: Callable[..., None],
    relative_path: Path | None = None,
) -> None:
    """Persist rejected identifiers using the pipeline's additional table hook."""

    rejected_df = pd.DataFrame(list(rows)).convert_dtypes()
    if rejected_df.empty:
        return

    output_path = relative_path or Path("qc") / "document_rejected_inputs.csv"
    logger.warning(
        "rejected_inputs_found",
        count=len(rejected_df),
        path=str(output_path),
    )

    add_table(
        "document_rejected_inputs",
        rejected_df,
        relative_path=output_path,
    )
