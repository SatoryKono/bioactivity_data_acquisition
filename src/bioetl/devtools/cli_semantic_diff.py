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
    methods: dict[str, Any] = {}

    code_block_pattern = re.compile(r"```python(.*?)```", re.DOTALL)
    for block in code_block_pattern.finditer(content):
        block_content = block.group(1)
        try:
            module = ast.parse(block_content)
        except SyntaxError:
            continue
        for node in module.body:
            if isinstance(node, ast.ClassDef) and node.name == "PipelineBase":
                for statement in node.body:
                    if isinstance(statement, ast.FunctionDef):
                        methods[statement.name] = signature_from_docs(
                            statement, empty_annotation=None
                        )
                return methods

    if not methods:
        return {"error": "PipelineBase definition not found in documentation"}

    return methods


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
    """Return a curated list of known CLI flags."""
    try:
        # Automatic extraction is complex; return curated defaults.
        return [
            {"name": "--config", "required": True, "description": "Path to config file"},
            {"name": "--output-dir", "required": True, "description": "Output directory"},
            {"name": "--dry-run", "required": False, "description": "Dry run mode"},
            {"name": "--limit", "required": False, "description": "Limit rows"},
            {"name": "--set", "required": False, "description": "Override config"},
            {"name": "--verbose", "required": False, "description": "Verbose output"},
        ]
    except Exception as exc:  # noqa: BLE001
        return [{"error": str(exc)}]


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
