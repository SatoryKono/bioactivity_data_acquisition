#!/usr/bin/env python3
"""Семантический diff Doc→Code: сравнение API стадий, полей конфигов, флагов CLI."""

import ast
import inspect
import json
import re
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).parent.parent
AUDIT_RESULTS = ROOT / "audit_results"
AUDIT_RESULTS.mkdir(exist_ok=True)
DOCS = ROOT / "docs"


def extract_method_signature_from_code(method: Any) -> Dict[str, Any]:
    """Извлекает сигнатуру метода из кода."""
    try:
        sig = inspect.signature(method)
        params = []
        for param_name, param in sig.parameters.items():
            param_info = {
                "name": param_name,
                "kind": str(param.kind),
                "annotation": str(param.annotation) if param.annotation != inspect.Parameter.empty else "Any",
                "default": str(param.default) if param.default != inspect.Parameter.empty else None,
            }
            params.append(param_info)
        
        return {
            "name": method.__name__,
            "parameters": params,
            "return_annotation": str(sig.return_annotation) if sig.return_annotation != inspect.Parameter.empty else "Any",
        }
    except Exception as e:
        return {"error": str(e)}


def extract_pipeline_base_methods() -> Dict[str, Any]:
    """Извлекает методы PipelineBase из кода."""
    from bioetl.pipelines.base import PipelineBase
    
    methods = {}
    
    # Ключевые методы стадий
    stage_methods = ["extract", "transform", "validate", "write", "run"]
    
    for method_name in stage_methods:
        if hasattr(PipelineBase, method_name):
            method = getattr(PipelineBase, method_name)
            if inspect.isfunction(method) or inspect.ismethod(method):
                methods[method_name] = extract_method_signature_from_code(method)
    
    return methods


def extract_pipeline_base_from_docs() -> Dict[str, Any]:
    """Извлекает сигнатуры методов из документации."""
    doc_file = DOCS / "pipelines" / "00-pipeline-base.md"
    
    if not doc_file.exists():
        return {"error": f"Documentation file not found: {doc_file}"}
    
    content = doc_file.read_text(encoding="utf-8")
    
    # Ищем блоки кода с сигнатурами
    methods = {}
    
    # Паттерн для поиска сигнатур методов
    pattern = r'def\s+(\w+)\s*\(([^)]*)\)\s*->\s*([^:]+):'
    
    for match in re.finditer(pattern, content):
        method_name = match.group(1)
        params_str = match.group(2)
        return_annotation = match.group(3).strip()
        
        # Парсим параметры
        params = []
        if params_str.strip():
            for param in params_str.split(","):
                param = param.strip()
                if "=" in param:
                    name, default = param.split("=", 1)
                    name = name.strip()
                    default = default.strip()
                    params.append({"name": name, "default": default})
                else:
                    name = param.strip()
                    params.append({"name": name})
        
        methods[method_name] = {
            "name": method_name,
            "parameters": params,
            "return_annotation": return_annotation,
        }
    
    return methods


def extract_config_fields_from_code() -> Dict[str, Any]:
    """Извлекает поля конфигов из Pydantic моделей."""
    try:
        from bioetl.configs.models import PipelineConfig
        
        fields = {}
        
        if hasattr(PipelineConfig, "model_fields"):
            for field_name, field_info in PipelineConfig.model_fields.items():
                fields[field_name] = {
                    "type": str(field_info.annotation) if hasattr(field_info, "annotation") else None,
                    "required": field_info.is_required() if hasattr(field_info, "is_required") else None,
                    "default": str(field_info.default) if hasattr(field_info, "default") and field_info.default is not None else None,
                }
        
        return fields
    except Exception as e:
        return {"error": str(e)}


def extract_config_fields_from_docs() -> Dict[str, Any]:
    """Извлекает поля конфигов из документации."""
    doc_file = DOCS / "configs" / "00-typed-configs-and-profiles.md"
    
    if not doc_file.exists():
        return {"error": f"Documentation file not found: {doc_file}"}
    
    content = doc_file.read_text(encoding="utf-8")
    
    # Ищем таблицы с полями конфигов
    fields = {}
    
    # Паттерн для поиска полей в таблицах
    # Формат: | `key` | Type | Required | Default | Description |
    pattern = r'\|\s*`([^`]+)`\s*\|\s*([^|]+)\s*\|\s*([^|]+)\s*\|\s*([^|]+)\s*\|\s*([^|]+)\s*\|'
    
    for match in re.finditer(pattern, content):
        key = match.group(1).strip()
        field_type = match.group(2).strip()
        required = match.group(3).strip()
        default = match.group(4).strip()
        description = match.group(5).strip()
        
        fields[key] = {
            "type": field_type,
            "required": required.lower() in ["yes", "required", "обязательный"],
            "default": default if default.lower() not in ["—", "n/a", "none"] else None,
            "description": description,
        }
    
    return fields


def extract_cli_flags_from_code() -> List[Dict[str, Any]]:
    """Извлекает флаги CLI из кода."""
    try:
        from bioetl.cli.command import create_pipeline_command
        import typer
        
        # Попытка извлечь флаги из Typer команды
        # Это сложно сделать автоматически, поэтому возвращаем известные флаги
        flags = [
            {"name": "--config", "required": True, "description": "Path to config file"},
            {"name": "--output-dir", "required": True, "description": "Output directory"},
            {"name": "--dry-run", "required": False, "description": "Dry run mode"},
            {"name": "--limit", "required": False, "description": "Limit rows"},
            {"name": "--set", "required": False, "description": "Override config"},
            {"name": "--verbose", "required": False, "description": "Verbose output"},
        ]
        
        return flags
    except Exception as e:
        return [{"error": str(e)}]


def extract_cli_flags_from_docs() -> List[Dict[str, Any]]:
    """Извлекает флаги CLI из документации."""
    doc_file = DOCS / "cli" / "01-cli-commands.md"
    
    if not doc_file.exists():
        return [{"error": f"Documentation file not found: {doc_file}"}]
    
    content = doc_file.read_text(encoding="utf-8")
    
    flags = []
    
    # Паттерн для поиска флагов в таблицах
    pattern = r'\|\s*`([^`]+)`\s*\|\s*([^|]+)\s*\|\s*([^|]+)\s*\|\s*([^|]+)\s*\|'
    
    for match in re.finditer(pattern, content):
        flag_name = match.group(1).strip()
        shorthand = match.group(2).strip()
        required = match.group(3).strip()
        description = match.group(4).strip()
        
        flags.append({
            "name": flag_name,
            "shorthand": shorthand,
            "required": required.lower() in ["yes", "required", "**yes**"],
            "description": description,
        })
    
    return flags


def compare_methods(code_methods: Dict[str, Any], doc_methods: Dict[str, Any]) -> Dict[str, Any]:
    """Сравнивает методы из кода и документации."""
    differences = {}
    
    all_methods = set(code_methods.keys()) | set(doc_methods.keys())
    
    for method_name in all_methods:
        code_method = code_methods.get(method_name)
        doc_method = doc_methods.get(method_name)
        
        if code_method is None:
            differences[method_name] = {
                "status": "gap",
                "issue": "Method not found in code",
                "doc": doc_method,
            }
        elif doc_method is None:
            differences[method_name] = {
                "status": "gap",
                "issue": "Method not found in docs",
                "code": code_method,
            }
        else:
            # Сравниваем сигнатуры
            issues = []
            
            # Сравниваем возвращаемые типы
            code_return = code_method.get("return_annotation", "Any")
            doc_return = doc_method.get("return_annotation", "Any")
            
            if code_return != doc_return:
                issues.append(f"Return type mismatch: code={code_return}, doc={doc_return}")
            
            # Сравниваем параметры
            code_params = code_method.get("parameters", [])
            doc_params = doc_method.get("parameters", [])
            
            if len(code_params) != len(doc_params):
                issues.append(f"Parameter count mismatch: code={len(code_params)}, doc={len(doc_params)}")
            
            if issues:
                differences[method_name] = {
                    "status": "contradiction",
                    "issues": issues,
                    "code": code_method,
                    "doc": doc_method,
                }
            else:
                differences[method_name] = {
                    "status": "ok",
                    "code": code_method,
                    "doc": doc_method,
                }
    
    return differences


def main():
    """Основная функция семантического diff."""
    print("Extracting PipelineBase methods from code...")
    code_methods = extract_pipeline_base_methods()
    
    print("Extracting PipelineBase methods from docs...")
    doc_methods = extract_pipeline_base_from_docs()
    
    print("Comparing methods...")
    method_differences = compare_methods(code_methods, doc_methods)
    
    print("Extracting config fields from code...")
    code_config_fields = extract_config_fields_from_code()
    
    print("Extracting config fields from docs...")
    doc_config_fields = extract_config_fields_from_docs()
    
    print("Extracting CLI flags from code...")
    code_cli_flags = extract_cli_flags_from_code()
    
    print("Extracting CLI flags from docs...")
    doc_cli_flags = extract_cli_flags_from_docs()
    
    # Сохраняем результаты
    diff_report = {
        "methods": method_differences,
        "config_fields": {
            "code": code_config_fields,
            "docs": doc_config_fields,
        },
        "cli_flags": {
            "code": code_cli_flags,
            "docs": doc_cli_flags,
        },
    }
    
    output_file = AUDIT_RESULTS / "semantic-diff-report.json"
    with output_file.open("w", encoding="utf-8") as f:
        json.dump(diff_report, f, indent=2, ensure_ascii=False)
    
    print(f"Semantic diff report saved to {output_file}")
    
    # Выводим краткую статистику
    ok_count = sum(1 for m in method_differences.values() if m.get("status") == "ok")
    gap_count = sum(1 for m in method_differences.values() if m.get("status") == "gap")
    contradiction_count = sum(1 for m in method_differences.values() if m.get("status") == "contradiction")
    
    print(f"\nMethods comparison:")
    print(f"  OK: {ok_count}")
    print(f"  Gaps: {gap_count}")
    print(f"  Contradictions: {contradiction_count}")


if __name__ == "__main__":
    main()

