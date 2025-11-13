"""Input/output configuration models."""

from __future__ import annotations

from collections.abc import Sequence

from pydantic import BaseModel, ConfigDict, Field


class IOInputConfig(BaseModel):
    """Configuration for reading input resources."""

    model_config = ConfigDict(extra="forbid")

    format: str = Field(default="csv", description="Input file format (csv, parquet, json).")
    encoding: str = Field(default="utf-8", description="Encoding for input files.")
    header: bool = Field(default=True, description="Whether the input file contains a header row.")
    path: str | None = Field(
        default=None,
        description="Explicit path to the local input file, when provided.",
    )


class IOOutputConfig(BaseModel):
    """Serialization settings for output artifacts."""

    model_config = ConfigDict(extra="forbid")

    format: str = Field(default="parquet", description="Output data format (parquet, csv).")
    partition_by: Sequence[str] = Field(
        default_factory=tuple,
        description="Columns used to partition the dataset.",
    )
    overwrite: bool = Field(
        default=True,
        description="Allow overwriting previously existing artifacts.",
    )
    path: str | None = Field(
        default=None,
        description="Explicit path to the output file or directory.",
    )


class IOConfig(BaseModel):
    """Unified input/output configuration for a pipeline."""

    model_config = ConfigDict(extra="forbid")

    input: IOInputConfig = Field(default_factory=IOInputConfig)
    output: IOOutputConfig = Field(default_factory=IOOutputConfig)
