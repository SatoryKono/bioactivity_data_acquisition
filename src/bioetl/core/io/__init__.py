"""
Core IO module for BioETL.

This module exposes key IO components like Artifacts, SchemaRegistry, and
lazy-loaded writers/services.
"""
from importlib import import_module
from typing import TYPE_CHECKING

from bioetl.core.io.artifacts import (
    DeterminismSettings,
    RunArtifacts,
    SchemaRegistry,
    SchemaRegistryEntry,
    WriteArtifacts,
    compute_file_hash,
    hash_business_key,
    hash_row,
)

if TYPE_CHECKING:
    from bioetl.core.io.output import (
        AtomicWriter,
        UnifiedOutputWriter,
        build_meta_yaml,
        emit_qc_artifact,
        validate_with_schema,
        write_json_atomic,
        write_yaml_atomic,
    )
    from bioetl.core.io.output_service import PipelineOutputService
    from bioetl.core.io.writer import ArtifactWriter

__all__ = [
    "ArtifactWriter",
    "AtomicWriter",
    "DeterminismSettings",
    "PipelineOutputService",
    "RunArtifacts",
    "SchemaRegistry",
    "SchemaRegistryEntry",
    "UnifiedOutputWriter",
    "WriteArtifacts",
    "build_meta_yaml",
    "compute_file_hash",
    "emit_qc_artifact",
    "hash_business_key",
    "hash_row",
    "validate_with_schema",
    "write_json_atomic",
    "write_yaml_atomic",
]


def __getattr__(name: str):  # pragma: no cover - thin lazy loader
    if name in {
        "ArtifactWriter",
        "AtomicWriter",
        "PipelineOutputService",
        "UnifiedOutputWriter",
        "build_meta_yaml",
        "emit_qc_artifact",
        "validate_with_schema",
        "write_json_atomic",
        "write_yaml_atomic",
    }:
        if name == "PipelineOutputService":
            module_name = "bioetl.core.io.output_service"
        elif name == "ArtifactWriter":
            module_name = "bioetl.core.io.writer"
        else:
            module_name = "bioetl.core.io.output"
        module = import_module(module_name)
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
