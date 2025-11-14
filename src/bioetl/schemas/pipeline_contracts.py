"""Helper utilities binding pipelines to their Pandera schemas."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from bioetl.schemas import SchemaRegistryEntry, get_schema


@dataclass(frozen=True, slots=True)
class PipelineSchemaContract:
    """Declarative contract describing pipeline ↔ schema bindings."""

    pipeline_code: str
    schema_out: str
    schema_in: str | None = None

    def out_schema(self) -> SchemaRegistryEntry:
        """Return the descriptor for the output schema."""

        return get_schema(self.schema_out)

    def in_schema(self) -> SchemaRegistryEntry | None:
        """Return the descriptor for the input schema when configured."""

        if self.schema_in is None:
            return None
        return get_schema(self.schema_in)


_PIPELINE_SCHEMA_CONTRACTS: Mapping[str, PipelineSchemaContract] = {
    "activity_chembl": PipelineSchemaContract(
        pipeline_code="activity_chembl",
        schema_out="bioetl.schemas.chembl_activity_schema.ActivitySchema",
    ),
    "assay_chembl": PipelineSchemaContract(
        pipeline_code="assay_chembl",
        schema_out="bioetl.schemas.chembl_assay_schema.AssaySchema",
    ),
    "document_chembl": PipelineSchemaContract(
        pipeline_code="document_chembl",
        schema_out="bioetl.schemas.chembl_document_schema.DocumentSchema",
    ),
    "target_chembl": PipelineSchemaContract(
        pipeline_code="target_chembl",
        schema_out="bioetl.schemas.chembl_target_schema.TargetSchema",
    ),
    "testitem_chembl": PipelineSchemaContract(
        pipeline_code="testitem_chembl",
        schema_out="bioetl.schemas.chembl_testitem_schema.TestItemSchema",
    ),
}


def _normalize_pipeline_code(pipeline_code: str) -> str:
    normalized = pipeline_code.strip()
    if not normalized:
        msg = "Pipeline code must be a non-empty string."
        raise ValueError(msg)
    return normalized.lower()


def get_pipeline_contract(pipeline_code: str) -> PipelineSchemaContract:
    """Return the declarative contract for ``pipeline_code``."""

    normalized = _normalize_pipeline_code(pipeline_code)
    try:
        return _PIPELINE_SCHEMA_CONTRACTS[normalized]
    except KeyError as exc:
        msg = f"Pipeline '{pipeline_code}' does not declare schema bindings."
        raise KeyError(msg) from exc


def get_out_schema(pipeline_code: str) -> SchemaRegistryEntry:
    """Return the output schema descriptor for ``pipeline_code``."""

    contract = get_pipeline_contract(pipeline_code)
    return contract.out_schema()


def get_in_schema(pipeline_code: str) -> SchemaRegistryEntry | None:
    """Return the input schema descriptor for ``pipeline_code`` when available."""

    contract = get_pipeline_contract(pipeline_code)
    return contract.in_schema()


def get_business_key_fields(pipeline_code: str) -> tuple[str, ...]:
    """Return declared business-key fields for the pipeline output schema."""

    descriptor = get_out_schema(pipeline_code)
    return descriptor.business_key_fields


def get_column_order(pipeline_code: str) -> tuple[str, ...]:
    """Return the canonical column order for the pipeline output schema."""

    descriptor = get_out_schema(pipeline_code)
    return descriptor.column_order

