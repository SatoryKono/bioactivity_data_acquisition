from __future__ import annotations

from pathlib import Path

import pytest

from bioetl.cli.cli_registry import (
    COMMAND_REGISTRY,
    PIPELINE_REGISTRY,
    TOOL_COMMANDS,
    CommandConfig,
)
from bioetl.pipelines.base import PipelineBase


def test_pipeline_registry_factories() -> None:
    for spec in PIPELINE_REGISTRY:
        factory = COMMAND_REGISTRY[spec.code]
        if spec.pipeline_path is None:
            with pytest.raises(NotImplementedError):
                factory()
            continue

        config = factory()
        assert isinstance(config, CommandConfig)
        assert config.name == spec.code
        assert config.canonical_name == spec.code
        assert config.description == spec.description
        assert issubclass(config.pipeline_class, PipelineBase)
        if spec.default_config is None:
            assert config.default_config_path is None
        else:
            assert isinstance(config.default_config_path, Path)
            assert config.default_config_path.as_posix() == spec.default_config


def test_tool_command_registry_metadata() -> None:
    expected_modules = {
        "audit_docs": ("bioetl-audit-docs", "bioetl.cli.tools.audit_docs"),
        "build_vocab_store": ("bioetl-build-vocab-store", "bioetl.cli.tools.build_vocab_store"),
        "dup_finder": ("bioetl-dup-finder", "bioetl.cli.tools.dup_finder"),
        "catalog_code_symbols": (
            "bioetl-catalog-code-symbols",
            "bioetl.cli.tools.catalog_code_symbols",
        ),
        "check_comments": ("bioetl-check-comments", "bioetl.cli.tools.check_comments"),
        "check_output_artifacts": (
            "bioetl-check-output-artifacts",
            "bioetl.cli.tools.check_output_artifacts",
        ),
        "create_matrix_doc_code": (
            "bioetl-create-matrix-doc-code",
            "bioetl.cli.tools.create_matrix_doc_code",
        ),
        "determinism_check": ("bioetl-determinism-check", "bioetl.cli.tools.determinism_check"),
        "doctest_cli": ("bioetl-doctest-cli", "bioetl.cli.tools.doctest_cli"),
        "inventory_docs": ("bioetl-inventory-docs", "bioetl.cli.tools.inventory_docs"),
        "link_check": ("bioetl-link-check", "bioetl.cli.tools.link_check"),
        "remove_type_ignore": ("bioetl-remove-type-ignore", "bioetl.cli.tools.remove_type_ignore"),
        "run_test_report": ("bioetl-run-test-report", "bioetl.cli.tools.run_test_report"),
        "schema_guard": ("bioetl-schema-guard", "bioetl.cli.tools.schema_guard"),
        "semantic_diff": ("bioetl-semantic-diff", "bioetl.cli.tools.semantic_diff"),
        "vocab_audit": ("bioetl-vocab-audit", "bioetl.cli.tools.vocab_audit"),
        "qc_boundary_check": (
            "bioetl-qc-boundary-check",
            "bioetl.cli.tools.qc_boundary_check",
        ),
    }
    assert set(TOOL_COMMANDS) == set(expected_modules)
    for key, (script_name, module_path) in expected_modules.items():
        tool_config = TOOL_COMMANDS[key]
        assert tool_config.name == script_name
        assert tool_config.module == module_path
        assert tool_config.attribute == "main"


