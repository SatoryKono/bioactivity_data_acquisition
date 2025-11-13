"""CLI boundary tests ensuring QC helpers stay within pipeline layer."""

from __future__ import annotations

from pathlib import Path
from textwrap import dedent

import pytest

from bioetl.cli.tools._qc_boundary import (
    QC_MODULE_PREFIX,
    Violation,
    collect_qc_boundary_violations,
)


@pytest.mark.cli
def test_cli_modules_do_not_import_qc_directly() -> None:
    """Ensure CLI code relies on pipelines instead of QC helpers directly."""
    violations = collect_qc_boundary_violations()
    assert not violations, _format_violation_message(violations)


def test_collect_qc_boundary_detects_indirect_imports(tmp_path: Path) -> None:
    """The analyzer must resolve re-export chains inside CLI tree."""
    cli_root = tmp_path / "cli_tool"
    cli_root.mkdir(parents=True, exist_ok=True)

    (cli_root / "__init__.py").write_text("", encoding="utf-8")
    (cli_root / "entry.py").write_text(
        dedent(
            """
            from .intermediate import export_qc_helper
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    (cli_root / "intermediate.py").write_text(
        dedent(
            """
            from bioetl.qc.helpers import build_report as export_qc_helper
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )

    package = "tmp_cli_tree"
    violations = collect_qc_boundary_violations(cli_root=cli_root, package=package)

    assert violations == [
        Violation(
            chain=(
                f"{package}.entry",
                "bioetl.qc.helpers",
            ),
            source_path=cli_root / "entry.py",
        ),
        Violation(
            chain=(
                f"{package}.intermediate",
                "bioetl.qc.helpers",
            ),
            source_path=cli_root / "intermediate.py",
        ),
    ]


def test_collect_qc_boundary_handles_direct_alias(tmp_path: Path) -> None:
    """Aliased direct imports should still be reported."""
    cli_root = tmp_path / "cli_alias"
    cli_root.mkdir(parents=True, exist_ok=True)

    (cli_root / "__init__.py").write_text("", encoding="utf-8")
    (cli_root / "command.py").write_text(
        dedent(
            """
            import bioetl.qc as qc
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )

    package = "tmp_cli_alias"
    violations = collect_qc_boundary_violations(cli_root=cli_root, package=package)

    assert violations == [
        Violation(
            chain=(
                f"{package}.command",
                QC_MODULE_PREFIX,
            ),
            source_path=cli_root / "command.py",
        ),
    ]


def _format_violation_message(violations: list[Violation]) -> str:
    formatted = [
        f"{violation.source_path}: {violation.format_chain()}" for violation in violations
    ]
    return (
        "CLI modules must not depend on bioetl.qc. Offending import chains:\n"
        + "\n".join(formatted)
    )

