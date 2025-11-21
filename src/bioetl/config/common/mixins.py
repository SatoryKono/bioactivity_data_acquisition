"""Reusable mixins for configuration models."""

from __future__ import annotations

from typing import ClassVar, TypeVar

from pydantic import model_validator

from ..models.source import enforce_positive_int_cap

ConfigT = TypeVar("ConfigT", bound="BatchSizeLimitMixin")


class BatchSizeLimitMixin:
    """Mixin that enforces an upper bound on batch-sized fields."""

    batch_field: ClassVar[str | None]
    default_batch_size: ClassVar[int | None]

    @model_validator(mode="after")
    def enforce_limits(self: ConfigT) -> ConfigT:
        """Clamp the configured batch size to the declared default limit."""

        if (
            self.batch_field is not None
            and self.default_batch_size is not None
        ):
            enforce_positive_int_cap(
                self, field=self.batch_field, cap=self.default_batch_size
            )
        return self


__all__ = ["BatchSizeLimitMixin"]
