"""CLI boundary tests ensuring QC helpers stay within pipeline layer."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

CLI_SOURCE_ROOT = Path("src") / "bioetl" / "cli"


def _collect_qc_imports() -> list[tuple[str, str]]:
    offenders: list[tuple[str, str]] = []
    for module_path in CLI_SOURCE_ROOT.rglob("*.py"):
        source = module_path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(module_path))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                module_name = node.module or ""
                if module_name.startswith("bioetl.qc"):
                    offenders.append((str(module_path), module_name))
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith("bioetl.qc"):
                        offenders.append((str(module_path), alias.name))
    return offenders


@pytest.mark.cli
def test_cli_modules_do_not_import_qc_directly() -> None:
    """Ensure CLI code relies on pipelines instead of QC helpers directly."""
    offenders = _collect_qc_imports()
    assert not offenders, f"CLI modules must not import bioetl.qc: {offenders}"

