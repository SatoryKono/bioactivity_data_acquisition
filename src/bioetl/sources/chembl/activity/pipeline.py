"""Compatibility wrapper for the relocated ChEMBL activity pipeline."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

__all__ = ["ActivityPipeline"]


if TYPE_CHECKING:  # pragma: no cover - import-time cycle guard for typing
    from bioetl.pipelines.chembl_activity import ActivityPipeline


def __getattr__(name: str) -> Any:  # pragma: no cover - simple delegation
    from bioetl.pipelines import chembl_activity as _chembl_activity

    try:
        return getattr(_chembl_activity, name)
    except AttributeError as exc:
        raise AttributeError(name) from exc
