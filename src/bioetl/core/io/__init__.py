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
        OutputPlan,
        OutputWriter,
        UnifiedOutputWriter,
        build_meta_yaml,
        emit_qc_artifact,
        validate_with_schema,
        write_json_atomic,
        write_yaml_atomic,
    )
except Exception:  # pragma: no cover - заглушки для ленивой загрузки
    AtomicWriter = None
    OutputPlan = None
    OutputWriter = None
    UnifiedOutputWriter = None
    build_meta_yaml = None
    emit_qc_artifact = None
    validate_with_schema = None
    write_json_atomic = None
    write_yaml_atomic = None

__all__ = [
    "DeterminismSettings",
    "RunArtifacts",
    "SchemaRegistry",
    "SchemaRegistryEntry",
    "WriteArtifacts",
    "compute_file_hash",
    "hash_business_key",
    "hash_row",
    "AtomicWriter",
    "OutputPlan",
    "OutputWriter",
    "UnifiedOutputWriter",
]


def __getattr__(name: str):  # pragma: no cover - thin lazy loader
    if name in {
        "AtomicWriter",
        "OutputPlan",
        "OutputWriter",
        "UnifiedOutputWriter",
        "build_meta_yaml",
        "emit_qc_artifact",
        "validate_with_schema",
        "write_json_atomic",
        "write_yaml_atomic",
    }:
        module = import_module("bioetl.core.io.output")
        return getattr(module, name)
    raise AttributeError(name)
