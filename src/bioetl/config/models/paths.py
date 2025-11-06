"""Path and materialization configuration models."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class PathsConfig(BaseModel):
    """Paths used by the pipeline runtime."""

    model_config = ConfigDict(extra="forbid")

    input_root: str = Field(default="data/input", description="Default directory for input assets.")
    output_root: str = Field(default="data/output", description="Default directory for pipeline outputs.")
    cache_root: str = Field(default=".cache", description="Root directory for transient cache files.")


class MaterializationConfig(BaseModel):
    """Settings controlling how artifacts are written to disk."""

    model_config = ConfigDict(extra="forbid")

    root: str = Field(default="data/output", description="Base directory for materialized datasets.")
    default_format: str = Field(
        default="parquet",
        description="Default output format for tabular data (e.g., parquet, csv).",
    )
    pipeline_subdir: str | None = Field(
        default=None,
        description="Optional subdirectory under the output root for this pipeline run.",
    )
    filename_template: str | None = Field(
        default=None,
        description="Optional template for dataset filenames (supports Jinja-style placeholders).",
    )

