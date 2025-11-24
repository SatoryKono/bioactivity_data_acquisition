"""Fallback configuration models.

``FallbacksConfig`` mirrors the merge-policy docs that describe how individual
``domain.sources.<name>`` entries (Crossref → PubMed → OpenAlex → ChEMBL for the
document pipeline, UniProt/IUPHAR for targets, etc.) are stitched together.
The docstrings below call out the same defaults and examples so the generated
schema reflects what operators see in the "Merge Policy Summary" tables.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, PositiveInt


class FallbacksConfig(BaseModel):
    """Global behavior toggles for fallback mechanisms.

    Each pipeline declares its participating providers under ``domain.sources``;
    the merge policy is simply the documented order of those sources. For
    example, documents merge Crossref → PubMed → OpenAlex → ChEMBL, while target
    enrichment prioritizes UniProt before IUPHAR. This model lets pipelines
    enable/disable the policy entirely or cap how many sources can be probed
    before surfacing a hard failure, keeping runtime behavior aligned with the
    written merge strategy.
    """

    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(
        default=True,
        description=(
            "Master toggle for walking the documented source order (for example "
            "Crossref→PubMed→OpenAlex→ChEMBL when reconciling documents). The "
            "default `true` keeps the merge policy active so gaps can be filled "
            "from downstream sources."
        ),
    )
    max_depth: PositiveInt | None = Field(
        default=None,
        description=(
            "Optional cap on how many entries from `domain.sources` the merge "
            "policy may traverse (e.g. `2` would allow Crossref→PubMed but stop "
            "before OpenAlex). Use `null` to honor the full policy."
        ),
    )
