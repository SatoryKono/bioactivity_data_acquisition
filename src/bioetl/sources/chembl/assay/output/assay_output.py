"""Output helpers and fallback handling for the assay pipeline."""

from __future__ import annotations

from typing import Any

import pandas as pd

from bioetl.core.logger import UnifiedLogger
from bioetl.utils.fallback import FallbackRecordBuilder, build_fallback_payload

from ..constants import ASSAY_CLASS_ENRICHMENT_WHITELIST, ASSAY_FALLBACK_BUSINESS_COLUMNS

logger = UnifiedLogger.get(__name__)


class AssayOutputWriter:
    """Encapsulate fallback creation and output frame shaping."""

    def __init__(self, context: dict[str, Any]) -> None:
        self._fallback_builder = FallbackRecordBuilder(
            business_columns=ASSAY_FALLBACK_BUSINESS_COLUMNS,
            context=context,
        )

    # ------------------------------------------------------------------
    # Context management
    # ------------------------------------------------------------------
    def update_release(self, chembl_release: str | None) -> None:
        """Update the cached release value used for fallbacks."""

        self._fallback_builder.context["chembl_release"] = chembl_release

    # ------------------------------------------------------------------
    # Fallback helpers
    # ------------------------------------------------------------------
    def register_fallback(
        self,
        assay_id: str,
        reason: str,
        error: Exception | None = None,
    ) -> dict[str, Any]:
        """Create and log a fallback payload for the provided assay ID."""

        record = self._create_fallback_record(assay_id, reason, error)
        logger.warning(
            "assay_fallback_created",
            assay_id=assay_id,
            reason=reason,
            error=str(error) if error else None,
        )
        return record

    def _create_fallback_record(
        self,
        assay_id: str,
        reason: str,
        error: Exception | None = None,
    ) -> dict[str, Any]:
        record = self._fallback_builder.record({"assay_chembl_id": assay_id})
        metadata = build_fallback_payload(
            entity="assay",
            reason=reason,
            error=error,
            source="ChEMBL_FALLBACK",
            context=self._fallback_builder.context,
        )
        record.update(metadata)
        return dict(record)

    # ------------------------------------------------------------------
    # Output shaping
    # ------------------------------------------------------------------
    def materialize_base(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    def materialize_parameters(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    def materialize_classifications(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            columns = ["assay_chembl_id", "row_subtype", "row_index"] + list(
                ASSAY_CLASS_ENRICHMENT_WHITELIST.values()
            )
            return pd.DataFrame(columns=columns)
        return self.expand_classification_columns(df)

    def expand_classification_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        for column in ASSAY_CLASS_ENRICHMENT_WHITELIST.values():
            if column not in df.columns:
                df[column] = pd.NA
        return df
