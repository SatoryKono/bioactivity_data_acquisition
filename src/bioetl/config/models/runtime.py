"""Runtime execution configuration models."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, PositiveInt


class RuntimeConfig(BaseModel):
    """Controls execution-level parameters shared across pipelines."""

    model_config = ConfigDict(extra="forbid")

    parallelism: PositiveInt = Field(
        default=4,
        description="Number of parallel workers for extract/transform stages.",
    )
    chunk_rows: PositiveInt = Field(
        default=100_000,
        description="Row chunk size used for batch source processing.",
    )
    dry_run: bool = Field(
        default=False,
        description="Verification mode without writing artifacts to external systems.",
    )
    seed: int = Field(
        default=42,
        description="Initial seed for random number generators to preserve determinism.",
    )
