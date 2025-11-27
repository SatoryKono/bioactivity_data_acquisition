from importlib import import_module

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
try:  # pragma: no cover - допускаем отсутствие тяжёлых зависимостей при импорте
    from bioetl.core.io.output import (
        AtomicWriter,
        UnifiedOutputWriter,
        build_meta_yaml,
        emit_qc_artifact,
        validate_with_schema,
        write_json_atomic,
        write_yaml_atomic,
    )
except Exception:  # pragma: no cover - заглушки для ленивой загрузки
    AtomicWriter = None
    UnifiedOutputWriter = None
    build_meta_yaml = None
    emit_qc_artifact = None
    validate_with_schema = None
    write_json_atomic = None
    write_yaml_atomic = None

try:
    from bioetl.core.io.output_service import PipelineOutputService
except Exception:
    PipelineOutputService = None

try:
    from bioetl.core.io.writer import ArtifactWriter
except Exception:
    ArtifactWriter = None

__all__ = [
    "ArtifactWriter",
    "DeterminismSettings",
    "PipelineOutputService",
    "RunArtifacts",
    "SchemaRegistry",
    "SchemaRegistryEntry",
    "WriteArtifacts",
    "compute_file_hash",
    "hash_business_key",
    "hash_row",
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
    raise AttributeError(name)
