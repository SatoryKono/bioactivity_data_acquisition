from __future__ import annotations

from pathlib import Path

MAPPING = {
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
    "remove_type_ignore": "cli_remove_type_ignore",
    "run_test_report": "cli_run_test_report",
    "schema_guard": "cli_schema_guard",
    "semantic_diff": "cli_semantic_diff",
    "vocab_audit": "cli_vocab_audit",
}


def replace_imports() -> None:
    files = list(Path("tests").rglob("*.py")) + list(Path("src").rglob("*.py"))
    for path in files:
        original = path.read_text(encoding="utf-8")
        updated = original
        for old, new in MAPPING.items():
            updated = updated.replace(
                f"from bioetl.tools import {old}",
                f"from bioetl.cli.tools._logic import {new} as {old}",
            )
            updated = updated.replace(
                f"import bioetl.tools.{old} as",
                f"from bioetl.cli.tools._logic import {new} as",
            )
            updated = updated.replace(
                f"from bioetl.tools.{old} import",
                f"from bioetl.cli.tools._logic.{new} import",
            )
            updated = updated.replace(
                f"import bioetl.tools.{old}",
                f"from bioetl.cli.tools._logic import {new} as {old}",
            )
        if updated != original:
            path.write_text(updated, encoding="utf-8")


if __name__ == "__main__":
    replace_imports()
import pathlib
patterns = [
    ("bioetl.schemas.chembl_document_enrichment_schema_schema:", "bioetl.schemas.chembl_document_enrichment_schema_schema:"),
    ("bioetl.schemas.chembl_document_enrichment_schema_schema.", "bioetl.schemas.chembl_document_enrichment_schema_schema."),
    ("bioetl.schemas.chembl_document_enrichment_schema\"", "bioetl.schemas.chembl_document_enrichment_schema_schema\""),
    ("bioetl.schemas.chembl_document_enrichment_schema_schema'", "bioetl.schemas.chembl_document_enrichment_schema_schema'"),
    ("bioetl.schemas.chembl_document_enrichment_schema_schema", "bioetl.schemas.chembl_document_enrichment_schema_schema"),
]
text_suffixes = {".py", ".md", ".yaml", ".yml", ".json", ".toml"}
changed = 0
for path in pathlib.Path('.').rglob('*'):
    if not path.is_file():
        continue
    if path.suffix.lower() not in text_suffixes:
        continue
    data = path.read_text(encoding='utf-8')
    new_data = data
    for old, new in patterns:
        new_data = new_data.replace(old, new)
    if new_data != data:
        path.write_text(new_data, encoding='utf-8')
        changed += 1
print(f"updated {changed} files")
