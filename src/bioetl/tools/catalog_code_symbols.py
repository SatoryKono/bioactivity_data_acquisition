"""Каталогизация сигнатур ключевых объектов кодовой базы."""

from __future__ import annotations

import inspect
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from bioetl.core.log_events import LogEvents
from bioetl.core.logger import UnifiedLogger
from bioetl.tools import get_project_root

__all__ = [
    "CodeCatalog",
    "catalog_code_symbols",
]


PROJECT_ROOT = get_project_root()


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _write_text_atomic(path: Path, content: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        handle.write(content)
        handle.flush()
    tmp.replace(path)


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.flush()
    tmp.replace(path)


def extract_method_signature(method: Any) -> dict[str, Any]:
    """Извлекает сигнатуру метода."""

    sig = inspect.signature(method)
    params: list[dict[str, Any]] = []
    for param_name, param in sig.parameters.items():
        params.append(
            {
                "name": param_name,
                "kind": str(param.kind),
                "annotation": (
                    str(param.annotation) if param.annotation != inspect.Parameter.empty else None
                ),
                "default": (
                    str(param.default) if param.default != inspect.Parameter.empty else None
                ),
            }
        )

    return {
        "name": method.__name__,
        "parameters": params,
        "return_annotation": (
            str(sig.return_annotation) if sig.return_annotation != inspect.Parameter.empty else None
        ),
        "is_abstract": bool(getattr(method, "__isabstractmethod__", False)),
    }


def extract_pipeline_base_signatures() -> dict[str, Any]:
    """Извлекает сигнатуры методов PipelineBase."""

    from bioetl.pipelines.base import PipelineBase

    signatures: dict[str, Any] = {}

    for name, method in inspect.getmembers(PipelineBase, predicate=inspect.isfunction):
        if name.startswith("_"):
            continue
        signatures[name] = extract_method_signature(method)

    for name, bound_method in inspect.getmembers(PipelineBase, predicate=inspect.ismethod):
        if name.startswith("_"):
            continue
        signatures.setdefault(name, extract_method_signature(bound_method))

    for name, attr in PipelineBase.__dict__.items():
        if name.startswith("_"):
            continue
        if inspect.isfunction(attr) or inspect.ismethod(attr):
            signatures.setdefault(name, extract_method_signature(attr))

    return signatures


def extract_config_models() -> dict[str, Any]:
    """Извлекает модели конфигов из Pydantic."""

    try:
        from bioetl.config.models import DeterminismConfig, PipelineConfig, PipelineMetadata
    except ImportError as exc:  # pragma: no cover - инфраструктурная ошибка
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
    """Извлекает CLI-команды из реестра."""

    try:
        from bioetl.cli.registry import COMMAND_REGISTRY
    except ImportError as exc:  # pragma: no cover
        return [f"Error: {exc}"]
    return sorted(COMMAND_REGISTRY.keys())


@dataclass(frozen=True)
class CodeCatalog:
    """Результат каталогизации кодовых сущностей."""

    pipeline_signatures: dict[str, Any]
    config_models: dict[str, Any]
    cli_commands: list[str]
    json_path: Path
    cli_path: Path


def catalog_code_symbols(artifacts_dir: Path | None = None) -> CodeCatalog:
    """Извлекает сущности и формирует артефакты в каталоге."""

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
