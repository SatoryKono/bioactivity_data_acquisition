"""Compatibility layer for document merge utilities."""

from bioetl.sources.document.merge.policy import (  # noqa: F401
    CASTERS,
    FIELD_PRECEDENCE,
    detect_conflicts,
    merge_with_precedence,
)

__all__ = [
    "FIELD_PRECEDENCE",
    "CASTERS",
    "merge_with_precedence",
    "detect_conflicts",
]
