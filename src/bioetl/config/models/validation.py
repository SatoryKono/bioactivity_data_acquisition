"""Validation configuration models."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, PositiveInt, model_validator


class ValidationConfig(BaseModel):
    """Pandera schema configuration."""

    model_config = ConfigDict(extra="allow")

    schema_in: str | None = Field(
        default=None,
        description="Dotted path to the Pandera schema validating extracted data.",
    )
    schema_out: str | None = Field(
        default=None,
        description="Dotted path to the Pandera schema validating transformed data.",
    )
    schema_in_version: str | None = Field(
        default=None,
        description="Expected version of the input schema (paired with schema_in).",
    )
    schema_out_version: str | None = Field(
        default=None,
        description="Expected version of the output schema (paired with schema_out).",
    )
    allow_schema_migration: bool = Field(
        default=False,
        description="If true, attempt to migrate datasets to the expected schema version.",
    )
    max_schema_migration_hops: PositiveInt = Field(
        default=3,
        description="Maximum number of migration hops allowed when reconciling schema versions.",
    )
    strict: bool = Field(
        default=True, description="If true, Pandera enforces column order and presence."
    )
    coerce: bool = Field(
        default=True, description="If true, Pandera coerces data types during validation."
    )

    @model_validator(mode="after")
    def _validate_version_contract(self) -> "ValidationConfig":
        if self.schema_in_version and not self.schema_in:
            msg = "validation.schema_in_version requires validation.schema_in to be set"
            raise ValueError(msg)
        if self.schema_out_version and not self.schema_out:
            msg = "validation.schema_out_version requires validation.schema_out to be set"
            raise ValueError(msg)
        if self.allow_schema_migration and not self.max_schema_migration_hops:
            msg = "validation.max_schema_migration_hops must be positive when migrations are allowed"
            raise ValueError(msg)
        return self
