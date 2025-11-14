"""CLI configuration models."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, PositiveInt


class CLIConfig(BaseModel):
    """Runtime overrides captured from the CLI layer."""

    model_config = ConfigDict(extra="forbid")

    profiles: Sequence[str] = Field(
        default_factory=tuple,
        description="Profiles requested via the --profile flag (in order).",
    )
    environment_profiles: Sequence[str] = Field(
        default_factory=tuple,
        description="Environment profile files applied automatically (in order).",
    )
    environment: str | None = Field(
        default=None,
        description="Name of the BIOETL_ENV environment that contributed overrides.",
    )
    dry_run: bool = Field(
        default=False, description="If true, skip the write/materialization stage."
    )
    limit: PositiveInt | None = Field(
        default=None,
        description="Optional limit applied to extracted records for sampling/testing.",
    )
    sample: PositiveInt | None = Field(
        default=None,
        description="Random sample size requested via the CLI.",
    )
    extended: bool = Field(
        default=False,
        description="If true, enable extended QC artifacts and metrics.",
    )
    date_tag: str | None = Field(
        default=None,
        description="Optional YYYYMMDD tag injected into deterministic artifact names.",
    )
    golden: str | None = Field(
        default=None,
        description="Path to golden dataset for bitwise determinism comparison.",
    )
    verbose: bool = Field(
        default=False,
        description="If true, enable verbose (DEBUG-level) logging output.",
    )
    fail_on_schema_drift: bool = Field(
        default=True,
        description="If true, schema drift raises an error; otherwise it is logged and execution continues.",
    )
    validate_columns: bool = Field(
        default=True,
        description="If true, enforce strict column validation during Pandera checks.",
    )
    input_file: str | None = Field(
        default=None,
        description="Optional path to input file (CSV/Parquet) containing IDs for batch extraction.",
    )
    set_overrides: Mapping[str, Any] = Field(
        default_factory=dict,
        description="Key/value overrides provided via --set CLI arguments.",
    )
