"""Pandera schema exports and registry accessors.

The canonical facade for registry lookups lives in
``bioetl.core.unified_schema``; this module keeps backward compatible
re-exports of the concrete Pandera models for callers that need direct access
to the schema classes.  Importing via the facade keeps the public API surface
stable while avoiding heavy imports during module initialisation.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

# Explicit imports for ActivitySchema, AssaySchema and TestItemSchema to ensure static type checkers can resolve them.
# These are imported eagerly (not via __getattr__) because static analyzers struggle with the dynamic fallback.
from bioetl.schemas.activity import ActivitySchema  # noqa: PLC0415
from bioetl.schemas.assay import AssaySchema  # noqa: PLC0415
from bioetl.schemas.target import (  # noqa: PLC0415
    ProteinClassSchema,
    TargetComponentSchema,
    TargetSchema,
    XrefSchema,
)
from bioetl.schemas.testitem import TestItemSchema  # noqa: PLC0415

__all__ = (
    "BaseSchema",
    "ActivitySchema",
    "DocumentRawSchema",
    "DocumentNormalizedSchema",
    "DocumentSchema",
    "DocumentInputSchema",
    "ActivityInputSchema",
    "AssayInputSchema",
    "CrossrefInputSchema",
    "OpenAlexInputSchema",
    "PubMedInputSchema",
    "SemanticScholarInputSchema",
    "PubChemInputSchema",
    "TargetInputSchema",
    "TestItemInputSchema",
    "UniProtInputSchema",
    "AssaySchema",
    "TestItemSchema",
    "TargetSchema",
    "TargetComponentSchema",
    "ProteinClassSchema",
    "XrefSchema",
    "IupharTargetSchema",
    "IupharClassificationSchema",
    "IupharGoldSchema",
    "UniProtSchema",
    "PubChemSchema",
    "SchemaRegistry",
    "schema_registry",
)

_SCHEMA_EXPORTS: dict[str, str] = {
    "BaseSchema": "bioetl.schemas.base",
    "ActivitySchema": "bioetl.schemas.activity",
    "DocumentRawSchema": "bioetl.schemas.document",
    "DocumentNormalizedSchema": "bioetl.schemas.document",
    "DocumentSchema": "bioetl.schemas.document",
    "DocumentInputSchema": "bioetl.schemas.input_schemas",
    "ActivityInputSchema": "bioetl.schemas.input_schemas",
    "AssayInputSchema": "bioetl.schemas.input_schemas",
    "CrossrefInputSchema": "bioetl.schemas.input_schemas",
    "OpenAlexInputSchema": "bioetl.schemas.input_schemas",
    "PubMedInputSchema": "bioetl.schemas.input_schemas",
    "SemanticScholarInputSchema": "bioetl.schemas.input_schemas",
    "PubChemInputSchema": "bioetl.schemas.input_schemas",
    "TargetInputSchema": "bioetl.schemas.input_schemas",
    "TestItemInputSchema": "bioetl.schemas.input_schemas",
    "UniProtInputSchema": "bioetl.schemas.input_schemas",
    "AssaySchema": "bioetl.schemas.assay",
    "TestItemSchema": "bioetl.schemas.testitem",
    "TargetSchema": "bioetl.schemas.target",
    "TargetComponentSchema": "bioetl.schemas.target",
    "ProteinClassSchema": "bioetl.schemas.target",
    "XrefSchema": "bioetl.schemas.target",
    "IupharTargetSchema": "bioetl.sources.iuphar.schema",
    "IupharClassificationSchema": "bioetl.sources.iuphar.schema",
    "IupharGoldSchema": "bioetl.sources.iuphar.schema",
    "UniProtSchema": "bioetl.schemas.uniprot",
    "PubChemSchema": "bioetl.sources.pubchem.schema",
    "SchemaRegistry": "bioetl.schemas.registry",
    "schema_registry": "bioetl.schemas.registry",
}

if TYPE_CHECKING:  # pragma: no cover - imported for static analysis only.
    # Re-imports for type checking (already imported above at runtime)
    from bioetl.schemas.activity import ActivitySchema
    from bioetl.schemas.assay import AssaySchema
    from bioetl.schemas.base import BaseSchema
    from bioetl.schemas.document import (
        DocumentNormalizedSchema,
        DocumentRawSchema,
        DocumentSchema,
    )
    from bioetl.schemas.input_schemas import (
        ActivityInputSchema,
        AssayInputSchema,
        CrossrefInputSchema,
        DocumentInputSchema,
        OpenAlexInputSchema,
        PubChemInputSchema,
        PubMedInputSchema,
        SemanticScholarInputSchema,
        TargetInputSchema,
        TestItemInputSchema,
        UniProtInputSchema,
    )
    from bioetl.schemas.registry import SchemaRegistry, schema_registry
    from bioetl.schemas.target import (
        ProteinClassSchema,
        TargetComponentSchema,
        TargetSchema,
        XrefSchema,
    )
    from bioetl.schemas.testitem import TestItemSchema
    from bioetl.schemas.uniprot import UniProtSchema
    from bioetl.sources.iuphar.schema import (
        IupharClassificationSchema,
        IupharGoldSchema,
        IupharTargetSchema,
    )
    from bioetl.sources.pubchem.schema import PubChemSchema


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
