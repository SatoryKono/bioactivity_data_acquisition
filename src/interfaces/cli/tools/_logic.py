"""Лоадер для устаревших CLI-инструментов в пространстве ``interfaces.cli.tools``."""

from __future__ import annotations

import sys
from importlib import import_module
from types import ModuleType
from typing import Final

__all__ = ["LEGACY_TOOL_MAP", "resolve_target", "load_tool_module"]

# Сопоставление старых имён CLI-инструментов к новым реализациям в bioetl.devtools.
LEGACY_TOOL_MAP: Final[dict[str, str]] = {
    "audit_docs": "bioetl.devtools.cli_audit_docs",
    "build_vocab_store": "bioetl.devtools.cli_build_vocab_store",
    "catalog_code_symbols": "bioetl.devtools.cli_catalog_code_symbols",
    "check_comments": "bioetl.devtools.cli_check_comments",
    "check_output_artifacts": "bioetl.devtools.cli_check_output_artifacts",
    "create_matrix_doc_code": "bioetl.devtools.cli_create_matrix_doc_code",
    "determinism_check": "bioetl.devtools.cli_determinism_check",
    "doctest_cli": "bioetl.devtools.cli_doctest_cli",
    "dup_finder": "bioetl.devtools.cli_dup_finder",
    "inventory_docs": "bioetl.devtools.cli_inventory_docs",
    "link_check": "bioetl.devtools.cli_link_check",
    "qc_boundary": "bioetl.devtools.cli_qc_boundary",
    "qc_boundary_check": "bioetl.devtools.cli_qc_boundary",
    "remove_type_ignore": "bioetl.devtools.cli_remove_type_ignore",
    "run_test_report": "bioetl.devtools.cli_run_test_report",
    "schema_guard": "bioetl.devtools.cli_schema_guard",
    "semantic_diff": "bioetl.devtools.cli_semantic_diff",
    "vocab_audit": "bioetl.devtools.cli_vocab_audit",
}


def resolve_target(tool_name: str) -> str:
    """Вернуть новый путь к модулю для устаревшего ``tool_name``."""

    try:
        return LEGACY_TOOL_MAP[tool_name]
    except KeyError as exc:  # pragma: no cover - защита от устаревших имён
        msg = f"Legacy CLI tool '{tool_name}' is not registered"
        raise ImportError(msg) from exc


def load_tool_module(tool_name: str) -> ModuleType:
    """Импортировать новый модуль и зарегистрировать его под старым Namespace."""

    module_path = resolve_target(tool_name)
    module = import_module(module_path)
    sys.modules.setdefault(f"{__package__}.{tool_name}", module)
    return module
