"""Helpers for constructing fallback rows used by the document pipeline."""

from __future__ import annotations

from typing import Any

from bioetl.utils.fallback import build_fallback_payload


def build_document_fallback_row(
    document_id: str,
    *,
    error_type: str,
    error_message: str,
    chembl_release: str | None,
    error: Exception | None = None,
) -> dict[str, Any]:
    """Return a standardised fallback row for the supplied identifier."""

    return build_fallback_payload(
        entity="document",
        reason="exception",
        error=error,
        source="DOCUMENT_FALLBACK",
        message=error_message,
        context={
            "document_chembl_id": document_id,
            "chembl_release": chembl_release,
            "fallback_error_code": error_type,
        },
    )
