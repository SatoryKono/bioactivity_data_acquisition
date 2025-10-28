"""Pandera schemas and schema registry."""

from bioetl.schemas.activity import ActivitySchema
from bioetl.schemas.assay import AssaySchema
from bioetl.schemas.base import BaseSchema
from bioetl.schemas.document import DocumentSchema
from bioetl.schemas.document_input import DocumentInputSchema
from bioetl.schemas.registry import SchemaRegistry, schema_registry
from bioetl.schemas.target import (
    ProteinClassSchema,
    TargetComponentSchema,
    TargetSchema,
    XrefSchema,
)
from bioetl.schemas.testitem import TestItemSchema

__all__ = [
    "BaseSchema",
    "ActivitySchema",
    "DocumentSchema",
    "DocumentInputSchema",
    "AssaySchema",
    "TestItemSchema",
    "TargetSchema",
    "TargetComponentSchema",
    "ProteinClassSchema",
    "XrefSchema",
    "SchemaRegistry",
    "schema_registry",
]

