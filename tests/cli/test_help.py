from __future__ import annotations

import typer
from typer.testing import CliRunner

from tools import audit_docs as audit_docs_cli
from tools import build_vocab_store as build_vocab_store_cli
from tools import catalog_code_symbols as catalog_code_symbols_cli
from tools import check_comments as check_comments_cli
from tools import check_output_artifacts as check_output_artifacts_cli
from tools import create_matrix_doc_code as create_matrix_doc_code_cli
from tools import determinism_check as determinism_check_cli
from tools import doctest_cli as doctest_cli_cli
from tools import inventory_docs as inventory_docs_cli
from tools import link_check as link_check_cli
from tools import remove_type_ignore as remove_type_ignore_cli
from tools import run_test_report as run_test_report_cli
from tools import schema_guard as schema_guard_cli
from tools import semantic_diff as semantic_diff_cli
from tools import vocab_audit as vocab_audit_cli

CLI_APPS: list[tuple[str, typer.Typer]] = [
    ("bioetl-audit-docs", audit_docs_cli.app),
    ("bioetl-build-vocab-store", build_vocab_store_cli.app),
    ("bioetl-catalog-code-symbols", catalog_code_symbols_cli.app),
    ("bioetl-check-comments", check_comments_cli.app),
    ("bioetl-check-output-artifacts", check_output_artifacts_cli.app),
    ("bioetl-create-matrix-doc-code", create_matrix_doc_code_cli.app),
    ("bioetl-determinism-check", determinism_check_cli.app),
    ("bioetl-doctest-cli", doctest_cli_cli.app),
    ("bioetl-inventory-docs", inventory_docs_cli.app),
    ("bioetl-link-check", link_check_cli.app),
    ("bioetl-remove-type-ignore", remove_type_ignore_cli.app),
    ("bioetl-run-test-report", run_test_report_cli.app),
    ("bioetl-schema-guard", schema_guard_cli.app),
    ("bioetl-semantic-diff", semantic_diff_cli.app),
    ("bioetl-vocab-audit", vocab_audit_cli.app),
]


def test_cli_apps_show_help(runner: CliRunner) -> None:
    for command_name, app in CLI_APPS:
        result = runner.invoke(app, ["--help"], prog_name=command_name)
        assert result.exit_code == 0, result.stdout
        assert f"Usage: {command_name}" in result.stdout

