from __future__ import annotations

from pathlib import Path

from bioetl.domain.qc.boundary_tools import (
    collect_qc_boundary_violations,
)


def _write_module(root: Path, relative: str, content: str) -> Path:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def test_collect_qc_boundary_violations_detects_direct_import(tmp_path: Path) -> None:
    cli_root = tmp_path / "cli"
    _write_module(cli_root, "__init__.py", "")
    source = _write_module(
        cli_root,
        "command.py",
        "import bioetl.qc.metrics\n",
    )

    violations = collect_qc_boundary_violations(
        cli_root=cli_root,
        package="test.cli",
    )

    assert [(violation.chain, violation.source_path) for violation in violations] == [
        (("test.cli.command", "bioetl.qc.metrics"), source),
    ]


def test_collect_qc_boundary_violations_tracks_transitive_imports(tmp_path: Path) -> None:
    cli_root = tmp_path / "cli"
    _write_module(cli_root, "__init__.py", "")
    _write_module(cli_root, "command.py", "from . import helpers\n")
    _write_module(cli_root, "helpers.py", "from .internal import runner\n")
    _write_module(cli_root, "internal/__init__.py", "")
    _write_module(
        cli_root,
        "internal/runner.py",
        "from bioetl.qc.metrics import Registry\n",
    )

    violations = collect_qc_boundary_violations(
        cli_root=cli_root,
        package="test.cli",
    )

    chains = {violation.chain for violation in violations}
    assert chains == {
        ("test.cli.command", "bioetl.qc.metrics"),
        ("test.cli.helpers", "bioetl.qc.metrics"),
        ("test.cli.internal.runner", "bioetl.qc.metrics"),
    }


def test_collect_qc_boundary_violations_deduplicates_qc_targets(tmp_path: Path) -> None:
    cli_root = tmp_path / "cli"
    _write_module(cli_root, "__init__.py", "")
    source = _write_module(
        cli_root,
        "command.py",
        """
import bioetl.qc.metrics
from bioetl.qc.metrics import stats
        """.strip()
        + "\n",
    )

    violations = collect_qc_boundary_violations(
        cli_root=cli_root,
        package="test.cli",
    )

    assert len(violations) == 1
    violation = violations[0]
    assert violation.chain == ("test.cli.command", "bioetl.qc.metrics")
    assert violation.source_path == source
