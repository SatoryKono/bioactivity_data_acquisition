#!/usr/bin/env python3
"""Извлечение сигнатур кода: PipelineBase, модели конфигов, CLI-команды."""

import ast
import inspect
import json
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).parent.parent
AUDIT_RESULTS = ROOT / "audit_results"
AUDIT_RESULTS.mkdir(exist_ok=True)


def extract_method_signature(method: Any) -> Dict[str, Any]:
    """Извлекает сигнатуру метода."""
    sig = inspect.signature(method)
    params = []
    for param_name, param in sig.parameters.items():
        param_info = {
            "name": param_name,
            "kind": str(param.kind),
            "annotation": str(param.annotation) if param.annotation != inspect.Parameter.empty else None,
            "default": str(param.default) if param.default != inspect.Parameter.empty else None,
        }
        params.append(param_info)
    
    return {
        "name": method.__name__,
        "parameters": params,
        "return_annotation": str(sig.return_annotation) if sig.return_annotation != inspect.Parameter.empty else None,
        "is_abstract": inspect.isabstract(method) if hasattr(method, "__isabstractmethod__") else False,
    }


def extract_pipeline_base_signatures() -> Dict[str, Any]:
    """Извлекает сигнатуры методов PipelineBase."""
    from bioetl.pipelines.base import PipelineBase
    
    signatures = {}
    
    # Получаем все методы класса
    for name, method in inspect.getmembers(PipelineBase, predicate=inspect.isfunction):
        if not name.startswith("_"):
            signatures[name] = extract_method_signature(method)
    
    # Получаем методы экземпляра
    for name, method in inspect.getmembers(PipelineBase, predicate=inspect.ismethod):
        if not name.startswith("_"):
            if name not in signatures:
                signatures[name] = extract_method_signature(method)
    
    # Получаем абстрактные методы
    for name, method in inspect.getmembers(PipelineBase, predicate=inspect.isfunction):
        if hasattr(method, "__isabstractmethod__") and method.__isabstractmethod__:
            signatures[name] = extract_method_signature(method)
    
    # Проверяем методы через __dict__
    for name, attr in PipelineBase.__dict__.items():
        if inspect.isfunction(attr) or inspect.ismethod(attr):
            if not name.startswith("_"):
                if name not in signatures:
                    signatures[name] = extract_method_signature(attr)
    
    return signatures


def extract_config_models() -> Dict[str, Any]:
    """Извлекает модели конфигов из Pydantic."""
    try:
        from bioetl.config.models import PipelineConfig, PipelineMetadata, DeterminismConfig
        
        models = {}
        
        # PipelineConfig
        if hasattr(PipelineConfig, "model_fields"):
            fields = {}
            for field_name, field_info in PipelineConfig.model_fields.items():
                fields[field_name] = {
                    "type": str(field_info.annotation) if hasattr(field_info, "annotation") else None,
                    "required": field_info.is_required() if hasattr(field_info, "is_required") else None,
                    "default": str(field_info.default) if hasattr(field_info, "default") and field_info.default is not None else None,
                }
            models["PipelineConfig"] = {"fields": fields}
        
        # PipelineMetadata
        if hasattr(PipelineMetadata, "model_fields"):
            fields = {}
            for field_name, field_info in PipelineMetadata.model_fields.items():
                fields[field_name] = {
                    "type": str(field_info.annotation) if hasattr(field_info, "annotation") else None,
                    "required": field_info.is_required() if hasattr(field_info, "is_required") else None,
                }
            models["PipelineMetadata"] = {"fields": fields}
        
        return models
    except ImportError as e:
        return {"error": f"Failed to import config models: {e}"}


def extract_cli_commands() -> List[str]:
    """Извлекает CLI-команды из registry."""
    try:
        from bioetl.cli.registry import COMMAND_REGISTRY
        return list(COMMAND_REGISTRY.keys())
    except ImportError as e:
        return [f"Error: {e}"]


def main():
    """Основная функция извлечения символов."""
    print("Extracting PipelineBase signatures...")
    pipeline_signatures = extract_pipeline_base_signatures()
    
    print("Extracting config models...")
    config_models = extract_config_models()
    
    print("Extracting CLI commands...")
    cli_commands = extract_cli_commands()
    
    # Сохраняем результаты
    code_signatures = {
        "pipeline_base": pipeline_signatures,
        "config_models": config_models,
        "cli_commands": cli_commands,
    }
    
    output_file = AUDIT_RESULTS / "code_signatures.json"
    with output_file.open("w", encoding="utf-8") as f:
        json.dump(code_signatures, f, indent=2, ensure_ascii=False)
    
    print(f"Code signatures saved to {output_file}")
    print(f"Found {len(pipeline_signatures)} PipelineBase methods")
    print(f"Found {len(cli_commands)} CLI commands: {', '.join(cli_commands)}")
    
    # Сохраняем CLI команды в текстовый файл
    cli_file = AUDIT_RESULTS / "cli_commands.txt"
    with cli_file.open("w", encoding="utf-8") as f:
        for cmd in cli_commands:
            f.write(f"{cmd}\n")
    
    print(f"CLI commands saved to {cli_file}")


if __name__ == "__main__":
    main()

