"""Pandera schema exports and registry accessors."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

__all__ = (
    "BaseSchema",
    "ActivitySchema",
    "DocumentRawSchema",
    "DocumentNormalizedSchema",
    "DocumentSchema",
    "DocumentInputSchema",
    "AssaySchema",
    "TestItemSchema",
    "TargetSchema",
    "TargetComponentSchema",
    "ProteinClassSchema",
    "XrefSchema",
    "IupharTargetSchema",
    "IupharClassificationSchema",
    "IupharGoldSchema",
    "SchemaRegistry",
    "schema_registry",
)

_SCHEMA_EXPORTS: dict[str, str] = {
    "BaseSchema": "bioetl.schemas.base",
    "ActivitySchema": "bioetl.schemas.activity",
    "DocumentRawSchema": "bioetl.schemas.document",
    "DocumentNormalizedSchema": "bioetl.schemas.document",
    "DocumentSchema": "bioetl.schemas.document",
    "DocumentInputSchema": "bioetl.schemas.document_input",
    "AssaySchema": "bioetl.schemas.assay",
    "TestItemSchema": "bioetl.schemas.testitem",
    "TargetSchema": "bioetl.schemas.target",
    "TargetComponentSchema": "bioetl.schemas.target",
    "ProteinClassSchema": "bioetl.schemas.target",
    "XrefSchema": "bioetl.schemas.target",
    "IupharTargetSchema": "bioetl.schemas.iuphar",
    "IupharClassificationSchema": "bioetl.schemas.iuphar",
    "IupharGoldSchema": "bioetl.schemas.iuphar",
    "SchemaRegistry": "bioetl.schemas.registry",
    "schema_registry": "bioetl.schemas.registry",
}

if TYPE_CHECKING:  # pragma: no cover - imported for static analysis only.
    from bioetl.schemas.activity import ActivitySchema
    from bioetl.schemas.assay import AssaySchema
    from bioetl.schemas.base import BaseSchema
    from bioetl.schemas.document import (
        DocumentNormalizedSchema,
        DocumentRawSchema,
        DocumentSchema,
    )
    from bioetl.schemas.document_input import DocumentInputSchema
    from bioetl.schemas.registry import SchemaRegistry, schema_registry
    from bioetl.schemas.target import (
        ProteinClassSchema,
        TargetComponentSchema,
        TargetSchema,
        XrefSchema,
    )
    from bioetl.schemas.testitem import TestItemSchema
    from bioetl.schemas.iuphar import (
        IupharClassificationSchema,
        IupharGoldSchema,
        IupharTargetSchema,
    )


def __getattr__(name: str) -> Any:
    """Resolve schema exports lazily to avoid import-time side effects."""

    try:
        module_name = _SCHEMA_EXPORTS[name]
    except KeyError as exc:  # pragma: no cover - mirrors normal attribute error behaviour.
        raise AttributeError(f"module 'bioetl.schemas' has no attribute {name!r}") from exc
    module = import_module(module_name)
    return getattr(module, name)


def __dir__() -> list[str]:  # pragma: no cover - trivial helper.
    """Ensure ``dir(bioetl.schemas)`` surfaces the documented exports."""

    return sorted(set(__all__))

