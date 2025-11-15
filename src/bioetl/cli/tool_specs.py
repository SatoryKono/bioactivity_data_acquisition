"""Declarative registry for CLI tool commands."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final, Iterable

__all__ = ["ToolCommandSpec", "TOOL_COMMAND_SPECS", "iter_tool_specs"]


@dataclass(frozen=True)
class ToolCommandSpec:
    """Declarative specification of a CLI tool command."""

    code: str
    script_name: str
    description: str
    implementation: str
    attribute: str = "main"

    @property
    def alias_module(self) -> str:
        """Return the module path exposed via ``bioetl.cli.tools`` package."""

        return f"bioetl.cli.tools.{self.code}"


TOOL_COMMAND_SPECS: Final[tuple[ToolCommandSpec, ...]] = (
    ToolCommandSpec(
        code="audit_docs",
        script_name="bioetl-audit-docs",
        description="Run documentation audit and collect reports.",
        implementation="bioetl.cli.tools.cli_audit_docs",
    ),
    ToolCommandSpec(
        code="build_vocab_store",
        script_name="bioetl-build-vocab-store",
        description="Assemble the aggregated ChEMBL vocabulary and export YAML.",
        implementation="bioetl.cli.tools.cli_build_vocab_store",
    ),
    ToolCommandSpec(
        code="dup_finder",
        script_name="bioetl-dup-finder",
        description="Detect duplicate and near-duplicate code fragments across the repo.",
        implementation="bioetl.cli.tools.cli_dup_finder",
    ),
    ToolCommandSpec(
        code="catalog_code_symbols",
        script_name="bioetl-catalog-code-symbols",
        description="Build the code entity catalog and related reports.",
        implementation="bioetl.cli.tools.cli_catalog_code_symbols",
    ),
    ToolCommandSpec(
        code="check_comments",
        script_name="bioetl-check-comments",
        description="Validate code comments and TODO markers.",
        implementation="bioetl.cli.tools.cli_check_comments",
    ),
    ToolCommandSpec(
        code="check_output_artifacts",
        script_name="bioetl-check-output-artifacts",
        description="Inspect the data/output directory and flag artifacts.",
        implementation="bioetl.cli.tools.cli_check_output_artifacts",
    ),
    ToolCommandSpec(
        code="create_matrix_doc_code",
        script_name="bioetl-create-matrix-doc-code",
        description="Generate the Doc<->Code matrix and export artifacts.",
        implementation="bioetl.cli.tools.cli_create_matrix_doc_code",
    ),
    ToolCommandSpec(
        code="determinism_check",
        script_name="bioetl-determinism-check",
        description="Execute two runs and compare their logs.",
        implementation="bioetl.cli.tools.cli_determinism_check",
    ),
    ToolCommandSpec(
        code="doctest_cli",
        script_name="bioetl-doctest-cli",
        description="Execute CLI examples and generate a report.",
        implementation="bioetl.cli.tools.cli_doctest_cli",
    ),
    ToolCommandSpec(
        code="inventory_docs",
        script_name="bioetl-inventory-docs",
        description="Collect a Markdown document inventory and compute hashes.",
        implementation="bioetl.cli.tools.cli_inventory_docs",
    ),
    ToolCommandSpec(
        code="link_check",
        script_name="bioetl-link-check",
        description="Verify documentation links via lychee.",
        implementation="bioetl.cli.tools.cli_link_check",
    ),
    ToolCommandSpec(
        code="remove_type_ignore",
        script_name="bioetl-remove-type-ignore",
        description="Remove type ignore directives from source files.",
        implementation="bioetl.cli.tools.cli_remove_type_ignore",
    ),
    ToolCommandSpec(
        code="run_test_report",
        script_name="bioetl-run-test-report",
        description="Generate pytest and coverage reports with metadata.",
        implementation="bioetl.cli.tools.cli_run_test_report",
    ),
    ToolCommandSpec(
        code="schema_guard",
        script_name="bioetl-schema-guard",
        description="Validate pipeline configs and the Pandera registry.",
        implementation="bioetl.cli.tools.cli_schema_guard",
    ),
    ToolCommandSpec(
        code="semantic_diff",
        script_name="bioetl-semantic-diff",
        description="Compare documentation and code to produce a diff.",
        implementation="bioetl.cli.tools.cli_semantic_diff",
    ),
    ToolCommandSpec(
        code="vocab_audit",
        script_name="bioetl-vocab-audit",
        description="Audit ChEMBL vocabularies and generate a report.",
        implementation="bioetl.cli.tools.cli_vocab_audit",
    ),
    ToolCommandSpec(
        code="qc_boundary_check",
        script_name="bioetl-qc-boundary-check",
        description="Static verification that prevents direct or indirect imports of bioetl.qc from the CLI layer.",
        implementation="bioetl.cli.tools.cli_qc_boundary_check",
    ),
)


def iter_tool_specs() -> Iterable[ToolCommandSpec]:
    """Return an immutable iterator over declared CLI tool specifications."""

    return iter(TOOL_COMMAND_SPECS)

