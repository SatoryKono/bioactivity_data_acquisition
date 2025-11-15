"""Catalogue signatures of key codebase entities."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from bioetl.core.logging import LogEvents, UnifiedLogger
from bioetl.tools import get_project_root
from .signatures import signature_from_callable

__all__ = [
    "CodeCatalog",
    "catalog_code_symbols",
]


PROJECT_ROOT = get_project_root()


def _ensure_dir(path: Path) -> None:
    """Create the directory at ``path`` if it does not exist."""
    path.mkdir(parents=True, exist_ok=True)


def _write_text_atomic(path: Path, content: str) -> None:
    """Write text content atomically to the specified path."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        handle.write(content)
        handle.flush()
    tmp.replace(path)


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    """Write JSON payload atomically preserving Unicode characters."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.flush()
    tmp.replace(path)


def extract_pipeline_base_signatures() -> dict[str, Any]:
    """Collect PipelineBase method signatures."""

    from bioetl.pipelines.base import PipelineBase

    signatures: dict[str, Any] = {}

    import inspect

    for name, method in inspect.getmembers(PipelineBase, predicate=inspect.isfunction):
        if name.startswith("_"):
            continue
        signatures[name] = signature_from_callable(method, include_abstract_flag=True)

    for name, bound_method in inspect.getmembers(PipelineBase, predicate=inspect.ismethod):
        if name.startswith("_"):
            continue
        signatures.setdefault(
            name, signature_from_callable(bound_method, include_abstract_flag=True)
        )

    for name, attr in PipelineBase.__dict__.items():
        if name.startswith("_"):
            continue
        if inspect.isfunction(attr) or inspect.ismethod(attr):
            signatures.setdefault(
                name, signature_from_callable(attr, include_abstract_flag=True)
            )

    return signatures


def extract_config_models() -> dict[str, Any]:
    """Collect Pydantic config model metadata."""

    try:
        from bioetl.config.models.models import PipelineConfig, PipelineMetadata
        from bioetl.config.models.policies import DeterminismConfig
    except ImportError as exc:  # pragma: no cover - infrastructure failure
        return {"error": f"Failed to import config models: {exc}"}

    def _model_fields(model: Any) -> dict[str, Any]:
        fields: dict[str, Any] = {}
        if hasattr(model, "model_fields"):
            for field_name, field_info in model.model_fields.items():
                fields[field_name] = {
                    "type": str(getattr(field_info, "annotation", None)),
                    "required": field_info.is_required()
                    if hasattr(field_info, "is_required")
                    else None,
                    "default": (
                        str(field_info.default)
                        if getattr(field_info, "default", None) is not None
                        else None
                    ),
                }
        return fields

    return {
        "PipelineConfig": {"fields": _model_fields(PipelineConfig)},
        "PipelineMetadata": {"fields": _model_fields(PipelineMetadata)},
        "DeterminismConfig": {"fields": _model_fields(DeterminismConfig)},
    }


def extract_cli_commands() -> list[str]:
    """Collect CLI command names from the registry."""

    try:
        from bioetl.cli.cli_registry import PIPELINE_REGISTRY
    except ImportError as exc:  # pragma: no cover
        return [f"Error: {exc}"]
    return sorted(spec.code for spec in PIPELINE_REGISTRY)


@dataclass(frozen=True)
class CodeCatalog:
    """Container describing collected code entities."""

    pipeline_signatures: dict[str, Any]
    config_models: dict[str, Any]
    cli_commands: list[str]
    json_path: Path
    cli_path: Path


def catalog_code_symbols(artifacts_dir: Path | None = None) -> CodeCatalog:
    """Extract code entities and write catalog artifacts."""

    UnifiedLogger.configure()
    log = UnifiedLogger.get(__name__)

    target_dir = artifacts_dir if artifacts_dir is not None else PROJECT_ROOT / "artifacts"
    _ensure_dir(target_dir)

    log.info(LogEvents.CATALOG_EXTRACT_START)
    pipeline_signatures = extract_pipeline_base_signatures()
    config_models = extract_config_models()
    cli_commands = extract_cli_commands()
    log.info(LogEvents.CATALOG_EXTRACT_DONE,
        pipeline_methods=len(pipeline_signatures),
        cli_commands=len(cli_commands),
    )

    code_signatures = {
        "pipeline_base": pipeline_signatures,
        "config_models": config_models,
        "cli_commands": cli_commands,
    }

    json_path = target_dir / "code_signatures.json"
    cli_path = target_dir / "cli_commands.txt"

    _write_json_atomic(json_path, code_signatures)
    _write_text_atomic(cli_path, "\n".join(cli_commands) + "\n")

    log.info(LogEvents.CATALOG_WRITTEN,
        json_path=str(json_path),
        cli_path=str(cli_path),
    )

    return CodeCatalog(
        pipeline_signatures=pipeline_signatures,
        config_models=config_models,
        cli_commands=cli_commands,
        json_path=json_path,
        cli_path=cli_path,
    )
