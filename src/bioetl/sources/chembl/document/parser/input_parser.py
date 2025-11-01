"""Helpers for parsing document pipeline inputs."""

from __future__ import annotations

import re
from typing import Any

import pandas as pd

from bioetl.core.logger import UnifiedLogger
from bioetl.schemas.input_schemas import DocumentInputSchema

logger = UnifiedLogger.get(__name__)


def prepare_document_input_ids(
    df: pd.DataFrame,
    *,
    schema: type[DocumentInputSchema] = DocumentInputSchema,
    pattern: re.Pattern[str] | None = None,
) -> tuple[list[str], list[dict[str, str]]]:
    """Validate and normalise document identifiers from the input frame."""

    if "document_chembl_id" not in df.columns:
        raise ValueError("Input file must contain 'document_chembl_id' column")

    compiled = pattern or re.compile(r"^CHEMBL\d+$")
    valid_ids: list[str] = []
    rejected: list[dict[str, str]] = []
    seen: set[str] = set()

    for raw_value in df["document_chembl_id"].tolist():
        normalised, reason = _normalise_identifier(raw_value, compiled)
        if reason:
            rejected.append(
                {
                    "document_chembl_id": "" if raw_value is None else str(raw_value),
                    "reason": reason,
                }
            )
            continue

        if normalised in seen:
            logger.debug("duplicate_id_skipped", document_chembl_id=normalised)
            continue

        if normalised is not None:
            seen.add(normalised)
            valid_ids.append(normalised)

    if valid_ids:
        schema.validate(pd.DataFrame({"document_chembl_id": valid_ids}))

    return valid_ids, rejected


def _normalise_identifier(
    value: Any,
    pattern: re.Pattern[str],
) -> tuple[str | None, str | None]:
    """Normalise identifier to uppercase CHEMBL format with validation reason."""

    if value is None or pd.isna(value):
        return None, "missing"

    text = str(value).strip().upper()
    if not text or text in {"#N/A", "N/A", "NONE", "NULL"}:
        return None, "missing"

    if not pattern.fullmatch(text):
        return None, "invalid_format"

    return text, None


__all__ = ["prepare_document_input_ids"]
