"""Output helpers for the ChEMBL activity pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

import pandas as pd

from bioetl.core.logger import UnifiedLogger
from bioetl.utils.qc import register_fallback_statistics

__all__ = ["ActivityOutputWriter"]

logger = UnifiedLogger.get(__name__)


class _PipelineLike(Protocol):
    qc_summary_data: dict[str, Any]

    def add_additional_table(
        self,
        name: str,
        dataframe: pd.DataFrame,
        *,
        relative_path: Path | None = None,
    ) -> None:
        ...

    def remove_additional_table(self, name: str) -> None:
        ...


_FALLBACK_COLUMNS: tuple[str, ...] = (
    "activity_id",
    "source_system",
    "fallback_reason",
    "fallback_error_type",
    "fallback_error_message",
    "fallback_http_status",
    "fallback_error_code",
    "fallback_retry_after_sec",
    "fallback_attempt",
    "fallback_timestamp",
    "chembl_release",
    "extracted_at",
)


class ActivityOutputWriter:
    """Capture and persist fallback diagnostics for activities."""

    def __init__(self, *, pipeline: _PipelineLike) -> None:
        self._pipeline = pipeline

    def capture_fallbacks(self, df: pd.DataFrame) -> dict[str, Any]:
        """Record fallback diagnostics and emit QC side-effects."""

        fallback_records = register_fallback_statistics(
            df,
            summary=self._pipeline.qc_summary_data,
            id_column="activity_id",
            fallback_columns=_FALLBACK_COLUMNS,
        )

        fallback_stats = self._pipeline.qc_summary_data.get("fallbacks", {}) or {}
        fallback_count = int(fallback_stats.get("fallback_count", 0))

        if fallback_count:
            logger.warning(
                "chembl_fallback_records_detected",
                count=fallback_count,
                activity_ids=fallback_stats.get("ids"),
                reasons=fallback_stats.get("reason_counts"),
            )
            self._pipeline.add_additional_table(
                "activity_fallback_records",
                fallback_records,
                relative_path=Path("qc") / "activity_fallback_records.csv",
            )
        else:
            self._pipeline.remove_additional_table("activity_fallback_records")

        return fallback_stats
