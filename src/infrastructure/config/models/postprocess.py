"""Post-processing configuration models.

This module documents knobs that run after the main transform/validation
phases. The defaults mirror the public pipeline specs so that generated schema
docs stay in sync with the written contract (for example the optional
correlation report toggle described in the Activity/Assay config guides).
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class PostprocessCorrelationConfig(BaseModel):
    """Post-processing options for correlation analysis.

    Pipelines expose the toggle as ``postprocess.correlation.enabled`` with the
    documented default of ``false`` (matching
    ``docs/pipelines/chembl/activity/17-activity-chembl-config.md``). When it
    stays ``false`` no extra artifacts are produced, keeping CI and smoke runs
    lean. Setting it to ``true`` (or passing ``--include-correlation`` via the
    CLI) instructs the QC planner to build a numeric correlation matrix and
    emit the ``<stem>_correlation_report.csv`` artifact using the shared
    ``runtime.correlation_template`` (``"{stem}_correlation_report.csv"`` by
    default).
    """

    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(
        default=False,
        description=(
            "Toggle the optional `<stem>_correlation_report.csv` artifact. The "
            "default `false` matches the published pipeline specs; setting it "
            "to `true` forces QC to compute and persist the correlation "
            "matrix using the runtime correlation template."
        ),
    )


class PostprocessConfig(BaseModel):
    """Top-level post-processing configuration.

    This section stays intentionally small: today it only surfaces correlation
    reports, but the surrounding documentation references it whenever a
    pipeline advertises post-transform data shaping (hashing, QC joins, merge
    audits, etc.). Keeping the docstring aligned with those docs ensures that
    generated schema references correctly describe how ``postprocess`` ties
    into downstream QC and merge policy reporting.
    """

    model_config = ConfigDict(extra="forbid")

    correlation: PostprocessCorrelationConfig = Field(
        default_factory=PostprocessCorrelationConfig,
        description=(
            "Correlation report controls (mirrors the documented `enabled=false` "
            "default and the `<stem>_correlation_report.csv` example)."
        ),
    )
