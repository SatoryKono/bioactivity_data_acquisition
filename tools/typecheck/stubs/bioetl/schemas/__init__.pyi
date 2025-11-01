from __future__ import annotations

from typing import ClassVar

from . import base as base


class ActivitySchema(base.BaseSchema):
    """Stub Pandera model representing the Activity output schema."""


class SchemaRegistry:
    """Lightweight typing stub mirroring the runtime schema registry."""

    _registry: ClassVar[dict[str, dict[str, type[base.BaseSchema]]]]

    @classmethod
    def register(
        cls,
        entity: str,
        schema_version: str,
        schema: type[base.BaseSchema],
        column_order: list[str] | None = ...,
        *,
        schema_id: str | None = ...,
        column_order_source: str | None = ...,
        na_policy: str | None = ...,
        precision_policy: str | None = ...,
    ) -> None:
        ...

    @classmethod
    def get(
        cls,
        entity: str,
        schema_version: str = ...,
    ) -> type[base.BaseSchema]:
        ...

    @classmethod
    def get_metadata(
        cls,
        entity: str,
        schema_version: str = ...,
    ) -> object | None:
        ...

    @classmethod
    def find_registration(
        cls,
        schema: type[base.BaseSchema],
    ) -> object | None:
        ...

    @classmethod
    def find_registration_by_schema_id(
        cls,
        schema_id: str,
        *,
        version: str | None = ...,
    ) -> object | None:
        ...


schema_registry: SchemaRegistry

__all__ = ["base", "ActivitySchema", "SchemaRegistry", "schema_registry"]
