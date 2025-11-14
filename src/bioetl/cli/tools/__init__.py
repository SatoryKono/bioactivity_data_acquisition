"""Совместимый namespace для CLI-инструментов `bioetl.cli.tools`."""

from __future__ import annotations

import sys
from importlib import import_module
from types import ModuleType
from typing import Final

_TOOL_ALIAS_TO_MODULE: Final[dict[str, str]] = {
    "audit_docs": "cli_audit_docs",
    "build_vocab_store": "cli_build_vocab_store",
    "catalog_code_symbols": "cli_catalog_code_symbols",
    "check_comments": "cli_check_comments",
    "check_output_artifacts": "cli_check_output_artifacts",
    "create_matrix_doc_code": "cli_create_matrix_doc_code",
    "determinism_check": "cli_determinism_check",
    "doctest_cli": "cli_doctest_cli",
    "dup_finder": "cli_dup_finder",
    "inventory_docs": "cli_inventory_docs",
    "link_check": "cli_link_check",
    "qc_boundary_check": "cli_qc_boundary_check",
    "qc_boundary": "cli_qc_boundary",
    "remove_type_ignore": "cli_remove_type_ignore",
    "run_test_report": "cli_run_test_report",
    "schema_guard": "cli_schema_guard",
    "semantic_diff": "cli_semantic_diff",
    "vocab_audit": "cli_vocab_audit",
}

__all__ = sorted(_TOOL_ALIAS_TO_MODULE.keys())


def _load_module(module_name: str) -> ModuleType:
    """Импортировать модуль CLI-инструмента с ленивой загрузкой."""

    return import_module(f"{__name__}.{module_name}")


for alias, module_name in _TOOL_ALIAS_TO_MODULE.items():
    module = _load_module(module_name)
    if hasattr(module, "cli_main") and not hasattr(module, "main"):
        module.main = module.cli_main  # type: ignore[attr-defined]
    globals()[alias] = module
    sys.modules[f"{__name__}.{alias}"] = module