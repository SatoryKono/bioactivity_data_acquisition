"""Quality control helpers exposed for pipeline orchestration.

The QC layer is designed for consumption exclusively from the pipeline layer.
CLI utilities and other entry points should invoke pipelines, which in turn
delegate to this module when building QC artefacts.
"""

from .metrics import compute_correlation_matrix, compute_duplicate_stats, compute_missingness
from .report import build_correlation_report, build_qc_metrics_payload, build_quality_report

__all__ = [
    "compute_correlation_matrix",
    "compute_duplicate_stats",
    "compute_missingness",
    "build_correlation_report",
    "build_quality_report",
    "build_qc_metrics_payload",
]
