"""Quality control helpers exposed for pipeline orchestration.

The QC layer is designed for consumption exclusively from the pipeline layer.
CLI utilities and other entry points should invoke pipelines, which in turn
delegate to this module when building QC artefacts.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any, Mapping

from .metrics import compute_correlation_matrix, compute_duplicate_stats, compute_missingness

if TYPE_CHECKING:
    from .report import (
        build_correlation_report,
        build_quality_report,
        build_qc_metrics_payload,
    )

_LAZY_EXPORTS: Mapping[str, str] = {
    "build_correlation_report": "bioetl.qc.report",
    "build_quality_report": "bioetl.qc.report",
    "build_qc_metrics_payload": "bioetl.qc.report",
}


def __getattr__(name: str) -> Any:
    module_name = _LAZY_EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(name)
    module = import_module(module_name)
    return getattr(module, name)


__all__ = [
    "compute_correlation_matrix",
    "compute_duplicate_stats",
    "compute_missingness",
    *sorted(_LAZY_EXPORTS),
]
