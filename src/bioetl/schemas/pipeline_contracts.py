"""Helper utilities binding pipelines to their Pandera schemas."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from bioetl.schemas import SchemaRegistryEntry, get_schema
from bioetl.schemas.metadata_utils import metadata_dict, normalize_sequence


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


def resolve_contract_sequence(
    contract: PipelineSchemaContract,
    key: str,
    fallback_attr: str,
) -> tuple[str, ...]:
    """Return metadata sequence ``key`` for ``contract`` with descriptor fallback."""

    descriptor = contract.out_schema()
    metadata = metadata_dict(descriptor.metadata)
    normalized = normalize_sequence(metadata.get(key))
    if normalized:
        return normalized

    if not hasattr(descriptor, fallback_attr):
        msg = f"Schema descriptor for '{contract.pipeline_code}' does not expose '{fallback_attr}'."
        raise AttributeError(msg)

    fallback_value = getattr(descriptor, fallback_attr)
    normalized_fallback = normalize_sequence(fallback_value)
    if normalized_fallback:
        return normalized_fallback
    if isinstance(fallback_value, tuple):
        return fallback_value
    if isinstance(fallback_value, list):
        return tuple(str(item) for item in fallback_value)
    if fallback_value is None:
        return ()
    return (str(fallback_value),)


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

    contract = get_pipeline_contract(pipeline_code)
    return resolve_contract_sequence(contract, "business_key_fields", "business_key_fields")


def get_column_order(pipeline_code: str) -> tuple[str, ...]:
    """Return the canonical column order for the pipeline output schema."""

    contract = get_pipeline_contract(pipeline_code)
    return resolve_contract_sequence(contract, "column_order", "column_order")

