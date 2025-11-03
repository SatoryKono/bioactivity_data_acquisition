"""Target pipeline namespace for ChEMBL source."""

from __future__ import annotations

from typing import Any

__all__ = ["TargetPipeline"]


def __getattr__(name: str) -> Any:  # pragma: no cover - simple delegation
    from bioetl.pipelines import chembl_target as _chembl_target

    try:
        return getattr(_chembl_target, name)
    except AttributeError as exc:
        raise AttributeError(name) from exc
