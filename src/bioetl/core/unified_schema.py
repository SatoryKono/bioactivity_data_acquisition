"""Typed facade for accessing the UnifiedSchema registry.

The ETL runtime keeps a single :class:`~bioetl.schemas.registry.SchemaRegistry`
instance that orchestrates schema registration, metadata discovery and column
order enforcement.  Historically callers reached into
``bioetl.schemas.registry`` directly which made it harder to evolve the public
API without touching dozens of modules.  This facade centralises the supported
helpers and gives static type checkers a stable surface to reason about.

Downstream code should import helpers from this module instead of coupling to
``bioetl.schemas`` implementation details.  Doing so keeps the import graph
shallow (avoiding heavyweight schema modules unless explicitly requested) and
simplifies the smoke tests that guard the public API.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from bioetl.pandera_pandas import DataFrameModel
from bioetl.schemas.activity import ActivitySchema
from bioetl.schemas.assay import AssaySchema
from bioetl.schemas.document import DocumentNormalizedSchema
from bioetl.schemas.registry import SchemaRegistration, SchemaRegistry, schema_registry
from bioetl.schemas.target import TargetSchema
from bioetl.schemas.testitem import TestItemSchema

__all__ = [
    "SchemaRegistration",
    "SchemaRegistry",
    "get_registry",
    "get_schema",
    "get_schema_metadata",
    "is_registered",
    "register_schema",
]


@dataclass(frozen=True)
class _SchemaRegistrationSpec:
    entity: str
    version: str
    schema: type[DataFrameModel]
    schema_id: str

    @property
    def column_order(self) -> list[str]:
        try:
            return list(self.schema.get_column_order())
        except Exception:  # pragma: no cover - defensive
            materialised = self.schema.to_schema()
            return list(materialised.columns.keys())  # type: ignore[union-attr]


_DEFAULT_SCHEMA_SPECS: tuple[_SchemaRegistrationSpec, ...] = (
    _SchemaRegistrationSpec(
        entity="document",
        version="1.0.0",
        schema=DocumentNormalizedSchema,
        schema_id="document.output",
    ),
    _SchemaRegistrationSpec(
        entity="assay",
        version="1.0.0",
        schema=AssaySchema,
        schema_id="assay.output",
    ),
    _SchemaRegistrationSpec(
        entity="activity",
        version="1.0.0",
        schema=ActivitySchema,
        schema_id="activity.output",
    ),
    _SchemaRegistrationSpec(
        entity="target",
        version="1.0.0",
        schema=TargetSchema,
        schema_id="target.output",
    ),
    _SchemaRegistrationSpec(
        entity="testitem",
        version="1.0.0",
        schema=TestItemSchema,
        schema_id="testitem.output",
    ),
)

_defaults_registered = False


def _ensure_default_registrations() -> None:
    """Register the default schema set exactly once."""

    global _defaults_registered
    if _defaults_registered:
        return

    for spec in _DEFAULT_SCHEMA_SPECS:
        metadata = schema_registry.get_metadata(spec.entity, spec.version)
        if metadata is None:
            schema_registry.register(
                spec.entity,
                spec.version,
                cast(DataFrameModel, spec.schema),
                list(spec.column_order),
                schema_id=spec.schema_id,
                column_order_source="unified_schema",
            )
    _defaults_registered = True


def get_registry() -> SchemaRegistry:
    """Return the singleton :class:`SchemaRegistry` instance used at runtime."""

    _ensure_default_registrations()
    return schema_registry


def register_schema(
    entity: str,
    schema_version: str,
    schema: type[DataFrameModel],
    *,
    column_order: list[str] | None = None,
    schema_id: str | None = None,
    column_order_source: str | None = None,
    na_policy: str | None = None,
    precision_policy: str | None = None,
) -> None:
    """Register ``schema`` for ``entity``.

    The helper enforces a typed signature so callers do not need to sprinkle
    ``# type: ignore`` comments when working with Pandera models.
    """

    _ensure_default_registrations()
    schema_registry.register(
        entity,
        schema_version,
        cast(DataFrameModel, schema),
        column_order,
        schema_id=schema_id,
        column_order_source=column_order_source,
        na_policy=na_policy,
        precision_policy=precision_policy,
    )


def get_schema(entity: str, schema_version: str = "latest") -> type[DataFrameModel]:
    """Return the Pandera model registered for ``entity``.

    Args:
        entity: Logical name of the dataset (``document``, ``activity`` …).
        schema_version: Semantic version recorded in the registry.  The special
            value ``"latest"`` resolves to the highest registered version.
    """

    _ensure_default_registrations()
    schema = schema_registry.get(entity, schema_version)
    return cast(type[DataFrameModel], schema)


def get_schema_metadata(
    entity: str,
    schema_version: str = "latest",
) -> SchemaRegistration | None:
    """Return metadata describing the registration for ``entity`` if present."""

    _ensure_default_registrations()
    return schema_registry.get_metadata(entity, schema_version)


def is_registered(entity: str, schema_version: str | None = None) -> bool:
    """Check whether ``entity`` is known to the registry.

    When ``schema_version`` is omitted the helper only verifies that *any*
    version exists.  Passing a concrete semantic version also validates that the
    requested revision is available.
    """

    _ensure_default_registrations()
    if schema_version is None:
        return schema_registry.get_metadata(entity) is not None

    try:
        schema_registry.get(entity, schema_version)
    except ValueError:
        return False
    return True
