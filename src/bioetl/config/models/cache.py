"""Cache configuration models."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, PositiveInt


class CacheConfig(BaseModel):
    """Configuration for the HTTP cache layer."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(default=True, description="Enable or disable the on-disk cache.")
    directory: str = Field(default="http_cache", description="Directory used to store cached data.")
    ttl: PositiveInt = Field(default=86_400, description="Time-to-live for cached entries in seconds.")

