"""Reusable mixins for configuration models."""

from __future__ import annotations

from typing import ClassVar, TypeVar

from pydantic import model_validator

from ..models.source import enforce_positive_int_cap

ConfigT = TypeVar("ConfigT", bound="BatchSizeLimitMixin")


class BatchSizeLimitMixin:
    """Mixin that enforces an upper bound on batch-sized fields."""

    batch_field: ClassVar[str]
    default_batch_size: ClassVar[int]

    @model_validator(mode="after")
    def enforce_limits(self: ConfigT) -> ConfigT:
        """Clamp the configured batch size to the declared default limit."""

        enforce_positive_int_cap(self, field=self.batch_field, cap=self.default_batch_size)
        return self


__all__ = ["BatchSizeLimitMixin"]
