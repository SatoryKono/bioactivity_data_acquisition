"""Deterministic I/O helpers for the BioETL core package."""

from .artifacts import RunArtifacts, WriteArtifacts, WriteResult
from .base_writer import BaseDatasetWriter
from .determinism import (
    CSVQuotingLiteral,
    DeterministicWriteArtifacts,
    build_write_artifacts,
    ensure_hash_columns,
    prepare_dataframe,
    serialise_metadata,
)
from .finalize_output import finalize_output
from .frame import ensure_columns
from .hashing import compute_hash, hash_from_mapping
from .output import plan_run_artifacts
from .writer import (
    build_run_manifest_payload,
    emit_qc_artifact,
    write_dataset_atomic,
    write_frame_like,
    write_json_atomic,
    write_yaml_atomic,
)
from .serialization import (
    escape_delims,
    header_rows_serialize,
    serialize_array_fields,
    serialize_objects,
    serialize_simple_list,
)
from .units import QCUnits

__all__ = [
    "BaseDatasetWriter",
    "CSVQuotingLiteral",
    "DeterministicWriteArtifacts",
    "QCUnits",
    "RunArtifacts",
    "WriteArtifacts",
    "WriteResult",
    "build_run_manifest_payload",
    "build_write_artifacts",
    "compute_hash",
    "ensure_columns",
    "ensure_hash_columns",
    "emit_qc_artifact",
    "escape_delims",
    "finalize_output",
    "hash_from_mapping",
    "header_rows_serialize",
    "plan_run_artifacts",
    "prepare_dataframe",
    "serialize_array_fields",
    "serialize_objects",
    "serialize_simple_list",
    "serialise_metadata",
    "write_dataset_atomic",
    "write_frame_like",
    "write_json_atomic",
    "write_yaml_atomic",
]
