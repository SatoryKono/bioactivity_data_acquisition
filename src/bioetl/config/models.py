from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict


class PipelineConfig(BaseModel):
    """Lightweight pipeline configuration wrapper.

    The model accepts arbitrary keys so that pipeline-specific options can be
    expressed without having to update the core schema for every change.
    """

    model_config = ConfigDict(extra="allow", frozen=True)

    name: str | None = None
    metadata: dict[str, Any] | None = None


__all__ = ["PipelineConfig"]
