"""Transform configuration models."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from pydantic import BaseModel, ConfigDict, Field


class TransformConfig(BaseModel):
    """Configuration for transform operations."""

    model_config = ConfigDict(extra="forbid")

    arrays_to_header_rows: Sequence[str] = Field(
        default_factory=tuple,
        description="List of column names to serialize from array-of-objects to header+rows format.",
    )
    enable_flatten: bool = Field(
        default=True,
        description="If true, enable flattening of nested objects into flat columns with prefixes.",
    )
    enable_serialization: bool = Field(
        default=True,
        description="If true, enable serialization of arrays to pipe-delimited or header+rows format.",
    )
    arrays_simple_to_pipe: Sequence[str] = Field(
        default_factory=tuple,
        description="List of column names containing simple arrays to serialize to pipe-delimited format.",
    )
    arrays_objects_to_header_rows: Sequence[str] = Field(
        default_factory=tuple,
        description="List of column names containing arrays of objects to serialize to header+rows format.",
    )
    flatten_objects: Mapping[str, Sequence[str]] = Field(
        default_factory=dict,
        description="Mapping of nested object column names to lists of field names to flatten.",
    )
