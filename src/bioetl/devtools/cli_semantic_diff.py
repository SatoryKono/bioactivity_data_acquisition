"""Perform semantic diffing between documentation and code contracts."""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path
from typing import Any

from bioetl.core.logging import LogEvents, UnifiedLogger
from bioetl.tools import get_project_root

from .signatures import signature_from_callable, signature_from_docs

__all__ = ["run_semantic_diff"]


PROJECT_ROOT = get_project_root()
ARTIFACTS_DIR = PROJECT_ROOT / "artifacts"
DOCS_ROOT = PROJECT_ROOT / "docs"


def extract_pipeline_base_methods() -> dict[str, Any]:
    """Extract PipelineBase method signatures from code."""
    from bioetl.core.pipeline import PipelineBase

    methods = {}
    for method_name in ["extract", "transform", "validate", "write", "run"]:
        if hasattr(PipelineBase, method_name):
            method = getattr(PipelineBase, method_name)
            if callable(method):
                methods[method_name] = signature_from_callable(
                    method, empty_annotation=None
                )
    return methods


def extract_pipeline_base_from_docs() -> dict[str, Any]:
    """Parse PipelineBase method definitions from documentation."""
    doc_file = DOCS_ROOT / "pipelines" / "00-pipeline-base.md"
    if not doc_file.exists():
        return {"error": f"Documentation file not found: {doc_file}"}

    content = doc_file.read_text(encoding="utf-8")
    target_method_names = {"extract", "transform", "validate", "write", "run"}

    code_block_pattern = re.compile(r"```python(.*?)```", re.DOTALL)
    
    # Собираем все определения класса PipelineBase из всех блоков
    all_class_definitions: list[dict[str, Any]] = []
    
    for block in code_block_pattern.finditer(content):
        block_content = block.group(1)
        try:
            module = ast.parse(block_content)
        except SyntaxError:
            continue
        
        for node in module.body:
            if isinstance(node, ast.ClassDef) and node.name == "PipelineBase":
                class_methods: dict[str, Any] = {}
                has_init = False
                
                for statement in node.body:
                    if isinstance(statement, ast.FunctionDef):
                        if statement.name == "__init__":
                            has_init = True
                        if statement.name in target_method_names:
                            class_methods[statement.name] = signature_from_docs(
                                statement, empty_annotation=None
                            )
                
                if class_methods:
                    all_class_definitions.append({
                        "methods": class_methods,
                        "has_init": has_init,
                        "method_count": len(class_methods),
                    })
    
    # Выбираем наиболее полное определение:
    # 1. Приоритет: определение с __init__ (более полное)
    # 2. Если нет __init__, выбираем с наибольшим количеством методов
    if not all_class_definitions:
        return {"error": "PipelineBase definition not found in documentation"}
    
    # Сначала ищем определение с __init__
    preferred = next(
        (defn for defn in all_class_definitions if defn["has_init"]),
        None
    )
    
    # Если нет определения с __init__, выбираем с наибольшим количеством методов
    if preferred is None:
        preferred = max(all_class_definitions, key=lambda x: x["method_count"])
    
    return preferred["methods"]


def extract_config_fields_from_code() -> dict[str, Any]:
    """Load typed config field definitions from the codebase."""
    try:
        from bioetl.config.models.models import PipelineConfig
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}

    fields: dict[str, Any] = {}
    if hasattr(PipelineConfig, "model_fields"):
        for field_name, field_info in PipelineConfig.model_fields.items():
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


def extract_config_fields_from_docs() -> dict[str, Any]:
    """Parse documented config field definitions."""
    doc_file = DOCS_ROOT / "configs" / "00-typed-configs-and-profiles.md"
    if not doc_file.exists():
        return {"error": f"Documentation file not found: {doc_file}"}

    content = doc_file.read_text(encoding="utf-8")
    fields: dict[str, Any] = {}
    pattern = r"\|\s*`([^`]+)`\s*\|\s*([^|]+)\s*\|\s*([^|]+)\s*\|\s*([^|]+)\s*\|\s*([^|]+)\s*\|"
    for match in re.finditer(pattern, content):
        key = match.group(1).strip()
        field_type = match.group(2).strip()
        required = match.group(3).strip()
        default = match.group(4).strip()
        description = match.group(5).strip()
        fields[key] = {
            "type": field_type,
            "required": required.lower() in ["yes", "required", "mandatory", "**yes**"],
            "default": None if default.lower() in ["—", "n/a", "none"] else default,
            "description": description,
        }
    return fields


def extract_cli_flags_from_code() -> list[dict[str, Any]]:
    """Extract CLI flags from create_pipeline_command function using AST parsing."""
    try:
        cli_file = PROJECT_ROOT / "src" / "bioetl" / "cli" / "cli_command.py"
        if not cli_file.exists():
            return [{"error": f"CLI file not found: {cli_file}"}]

        content = cli_file.read_text(encoding="utf-8")
        module = ast.parse(content)

        flags: list[dict[str, Any]] = []

        # Находим функцию create_pipeline_command
        for node in module.body:
            if isinstance(node, ast.FunctionDef) and node.name == "create_pipeline_command":
                # Ищем вложенную функцию command
                for stmt in node.body:
                    if isinstance(stmt, ast.FunctionDef) and stmt.name == "command":
                        # Парсим параметры функции command
                        for arg in stmt.args.args:
                            if arg.annotation:
                                # Находим соответствующий default (если есть)
                                arg_idx = stmt.args.args.index(arg)
                                default_value = None
                                if arg_idx < len(stmt.args.defaults):
                                    default_value = stmt.args.defaults[arg_idx]

                                # Проверяем, является ли default вызовом typer.Option
                                if (
                                    default_value
                                    and isinstance(default_value, ast.Call)
                                    and isinstance(default_value.func, ast.Attribute)
                                    and isinstance(default_value.func.value, ast.Name)
                                    and default_value.func.value.id == "typer"
                                    and default_value.func.attr == "Option"
                                ):
                                    flag_info = _parse_typer_option(
                                        default_value, arg.annotation
                                    )
                                    if flag_info:
                                        flags.append(flag_info)
                        break

        return flags if flags else [
            {"error": "No CLI flags found in create_pipeline_command"}
        ]
    except Exception as exc:  # noqa: BLE001
        return [{"error": str(exc)}]


def _parse_typer_option(call_node: ast.Call, annotation: ast.expr) -> dict[str, Any] | None:
    """Parse a typer.Option() call node and extract flag information."""
    try:
        # Первый позиционный аргумент - это default value
        # Второй и далее - это имена флагов (--flag, -f)
        # help - это keyword argument

        flag_name: str | None = None
        shorthand: str | None = None
        description: str | None = None
        required = False

        # Проверяем первый позиционный аргумент (default value)
        if call_node.args:
            first_arg = call_node.args[0]
            # Если это Ellipsis (...), то флаг обязательный
            if isinstance(first_arg, ast.Constant) and first_arg.value is ...:
                required = True
            elif isinstance(first_arg, ast.Constant):
                # Есть дефолтное значение, значит не обязательный
                required = False
            else:
                # Сложное выражение (например, default_config if ... else ...)
                # Считаем обязательным, если есть Ellipsis в выражении
                required = _has_ellipsis(first_arg)

        # Ищем строковые аргументы (имена флагов)
        for arg in call_node.args[1:]:
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                if arg.value.startswith("--"):
                    flag_name = arg.value
                elif arg.value.startswith("-") and len(arg.value) == 2:
                    shorthand = arg.value

        # Ищем keyword arguments
        for kw in call_node.keywords:
            if kw.arg == "help" and isinstance(kw.value, ast.Constant):
                if isinstance(kw.value.value, str):
                    description = kw.value.value
            elif kw.arg is None:  # **kwargs
                # Пропускаем
                pass

        if flag_name:
            return {
                "name": flag_name,
                "shorthand": shorthand,
                "required": required,
                "description": description or "",
            }
        return None
    except Exception:  # noqa: BLE001
        return None


def _has_ellipsis(node: ast.AST) -> bool:
    """Check if AST node contains Ellipsis (...)."""
    if isinstance(node, ast.Constant) and node.value is ...:
        return True
    for child in ast.iter_child_nodes(node):
        if _has_ellipsis(child):
            return True
    return False


def extract_cli_flags_from_docs() -> list[dict[str, Any]]:
    """Parse CLI flag definitions from documentation tables."""
    doc_file = DOCS_ROOT / "cli" / "01-cli-commands.md"
    if not doc_file.exists():
        return [{"error": f"Documentation file not found: {doc_file}"}]

    content = doc_file.read_text(encoding="utf-8")
    flags: list[dict[str, Any]] = []
    pattern = r"\|\s*`([^`]+)`\s*\|\s*([^|]+)\s*\|\s*([^|]+)\s*\|\s*([^|]+)\s*\|"
    for match in re.finditer(pattern, content):
        flag_name = match.group(1).strip()
        shorthand = match.group(2).strip()
        required = match.group(3).strip()
        description = match.group(4).strip()
        flags.append(
            {
                "name": flag_name,
                "shorthand": shorthand,
                "required": required.lower() in ["yes", "required", "**yes**"],
                "description": description,
            }
        )
    return flags


def compare_methods(code_methods: dict[str, Any], doc_methods: dict[str, Any]) -> dict[str, Any]:
    """Compare code signatures against documented signatures."""
    differences: dict[str, Any] = {}
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
            continue
        if doc_method is None:
            differences[method_name] = {
                "status": "gap",
                "issue": "Method not found in docs",
                "code": code_method,
            }
            continue

        issues = []
        if code_method.get("return_annotation") != doc_method.get("return_annotation"):
            issues.append(
                "Return type mismatch: code="
                f"{code_method.get('return_annotation')}, doc={doc_method.get('return_annotation')}"
            )

        code_params = code_method.get("parameters", [])
        doc_params = doc_method.get("parameters", [])
        if len(code_params) != len(doc_params):
            issues.append(
                f"Parameter count mismatch: code={len(code_params)}, doc={len(doc_params)}"
            )

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


def run_semantic_diff() -> Path:
    """Run semantic diff and return the path to the generated report."""

    UnifiedLogger.configure()
    log = UnifiedLogger.get(__name__)

    log.info(LogEvents.SEMANTIC_DIFF_EXTRACT_START)
    code_methods = extract_pipeline_base_methods()
    doc_methods = extract_pipeline_base_from_docs()
    method_differences = compare_methods(code_methods, doc_methods)

    code_config_fields = extract_config_fields_from_code()
    doc_config_fields = extract_config_fields_from_docs()

    code_cli_flags = extract_cli_flags_from_code()
    doc_cli_flags = extract_cli_flags_from_docs()

    diff_report = {
        "methods": method_differences,
        "config_fields": {"code": code_config_fields, "docs": doc_config_fields},
        "cli_flags": {"code": code_cli_flags, "docs": doc_cli_flags},
    }

    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    output_file = ARTIFACTS_DIR / "semantic-diff-report.json"
    tmp = output_file.with_suffix(output_file.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        json.dump(diff_report, handle, indent=2, ensure_ascii=False)
    tmp.replace(output_file)

    log.info(LogEvents.SEMANTIC_DIFF_WRITTEN, path=str(output_file))
    return output_file
