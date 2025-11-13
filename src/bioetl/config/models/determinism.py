"""Determinism configuration models."""

from __future__ import annotations

from collections.abc import Sequence

from pydantic import BaseModel, ConfigDict, Field, PositiveInt, model_validator


class DeterminismSerializationCSVConfig(BaseModel):
    """CSV serialization preferences."""

    model_config = ConfigDict(extra="forbid")

    separator: str = Field(default=",", description="Column separator used when writing CSV files.")
    quoting: str = Field(
        default="ALL", description="Quoting strategy compatible with pandas CSV writer."
    )
    na_rep: str = Field(
        default="", description="String representation for missing values in CSV output."
    )


class DeterminismSerializationConfig(BaseModel):
    """Normalization of serialized outputs."""

    model_config = ConfigDict(extra="forbid")

    csv: DeterminismSerializationCSVConfig = Field(
        default_factory=DeterminismSerializationCSVConfig
    )
    booleans: tuple[str, str] = Field(
        default=("True", "False"),
        description="Canonical string representations for boolean values.",
    )
    nan_rep: str = Field(default="NaN", description="String representation for NaN values.")


class DeterminismSortingConfig(BaseModel):
    """Stable sorting configuration."""

    model_config = ConfigDict(extra="forbid")

    by: list[str] = Field(
        default_factory=list, description="Columns defining the deterministic sort order."
    )
    ascending: list[bool] = Field(
        default_factory=list,
        description="Sort direction per column; defaults to ascending when empty.",
    )
    na_position: str = Field(
        default="last", description="Where to place null values during sorting."
    )


class DeterminismHashColumnSchema(BaseModel):
    """Schema definition for hash columns emitted by the pipeline."""

    model_config = ConfigDict(extra="forbid")

    dtype: str = Field(default="string", description="Column data type (pandas dtype).")
    length: PositiveInt = Field(default=64, description="Fixed hash length in characters.")
    nullable: bool = Field(default=False, description="Whether NULL/NA values are allowed.")


class DeterminismHashingConfig(BaseModel):
    """Hashing policy for determinism checks."""

    model_config = ConfigDict(extra="forbid")

    algorithm: str = Field(
        default="sha256", description="Hash algorithm used for row/business key hashes."
    )
    row_fields: Sequence[str] = Field(
        default_factory=tuple,
        description="Columns included in the per-row hash calculation.",
    )
    business_key_fields: Sequence[str] = Field(
        default_factory=tuple,
        description="Columns used to compute the business key hash.",
    )
    exclude_fields: Sequence[str] = Field(
        default_factory=lambda: ("generated_at", "run_id"),
        description="Fields excluded from deterministic hashing.",
    )
    business_key_column: str = Field(
        default="hash_business_key",
        description="Column storing the business-key hash in the final dataset.",
    )
    row_hash_column: str = Field(
        default="hash_row",
        description="Column storing the per-row hash in the final dataset.",
    )
    business_key_schema: DeterminismHashColumnSchema = Field(
        default_factory=DeterminismHashColumnSchema,
        description="Pandera/project schema for hash_business_key.",
    )
    row_hash_schema: DeterminismHashColumnSchema = Field(
        default_factory=DeterminismHashColumnSchema,
        description="Pandera/project schema for hash_row.",
    )


class DeterminismEnvironmentConfig(BaseModel):
    """Locale and timezone controls for deterministic output."""

    model_config = ConfigDict(extra="forbid")

    timezone: str = Field(default="UTC", description="Timezone enforced during pipeline execution.")
    locale: str = Field(default="C", description="Locale to apply when formatting values.")


class DeterminismWriteConfig(BaseModel):
    """Atomic write strategy."""

    model_config = ConfigDict(extra="forbid")

    strategy: str = Field(default="atomic", description="Write strategy (atomic or direct).")


class DeterminismMetaConfig(BaseModel):
    """Metadata emission controls."""

    model_config = ConfigDict(extra="forbid")

    location: str = Field(
        default="sibling",
        description="Where to store the generated meta.yaml relative to the dataset.",
    )
    include_fields: Sequence[str] = Field(
        default_factory=tuple,
        description="Metadata keys that must always be present.",
    )
    exclude_fields: Sequence[str] = Field(
        default_factory=tuple,
        description="Metadata keys that should be stripped before hashing.",
    )


class DeterminismConfig(BaseModel):
    """Deterministic output guarantees."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(default=True, description="Toggle determinism enforcement.")
    hash_policy_version: str = Field(
        default="1.0.0",
        description="Version of the hashing policy for compatibility tracking.",
    )
    float_precision: PositiveInt = Field(
        default=6,
        description="Number of decimal places to preserve when normalizing floats.",
    )
    datetime_format: str = Field(
        default="iso8601",
        description="Datetime serialization format used throughout the pipeline.",
    )
    column_validation_ignore_suffixes: Sequence[str] = Field(
        default_factory=lambda: ("_scd", "_temp", "_meta", "_tmp"),
        description="Column suffixes ignored during strict schema validation.",
    )
    sort: DeterminismSortingConfig = Field(default_factory=DeterminismSortingConfig)
    column_order: Sequence[str] = Field(
        default_factory=tuple,
        description="Expected column order for the final dataset.",
    )
    serialization: DeterminismSerializationConfig = Field(
        default_factory=DeterminismSerializationConfig
    )
    hashing: DeterminismHashingConfig = Field(default_factory=DeterminismHashingConfig)
    environment: DeterminismEnvironmentConfig = Field(default_factory=DeterminismEnvironmentConfig)
    write: DeterminismWriteConfig = Field(default_factory=DeterminismWriteConfig)
    meta: DeterminismMetaConfig = Field(default_factory=DeterminismMetaConfig)

    @model_validator(mode="after")
    def validate_sorting(self) -> DeterminismConfig:
        if self.sort.ascending and len(self.sort.ascending) != len(self.sort.by):
            msg = "determinism.sort.ascending must be empty or match determinism.sort.by length"
            raise ValueError(msg)
        if len(self.sort.by) != len(set(self.sort.by)):
            msg = "determinism.sort.by must not contain duplicate columns"
            raise ValueError(msg)
        if len(self.column_order) != len(set(self.column_order)):
            msg = "determinism.column_order must not contain duplicate columns"
            raise ValueError(msg)
        return self
