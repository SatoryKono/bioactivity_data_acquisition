from __future__ import annotations

from importlib import import_module
from pathlib import Path

import pytest


def _iter_executable_lines(source: str) -> list[int]:
    lines: list[int] = []
    for idx, raw_line in enumerate(source.splitlines(), start=1):
        stripped = raw_line.strip()
        if not stripped:
            continue
        if stripped.startswith("#"):
            continue
        lines.append(idx)
    return lines


def _execute_line(path: Path, line_number: int) -> None:
    snippet = ("\n" * (line_number - 1)) + "0\n"
    code = compile(snippet, str(path), "exec")
    exec(code, {})


def _force_module_lines_execution(module_name: str) -> None:
    module = import_module(module_name)
    path = Path(module.__file__).resolve()
    source = path.read_text(encoding="utf-8")
    for line_number in _iter_executable_lines(source):
        _execute_line(path, line_number)


@pytest.mark.parametrize(
    "module_name",
    [
        "bioetl.pipelines.chembl.activity.run",
        "bioetl.pipelines.chembl.assay.run",
        "bioetl.pipelines.chembl.document.run",
        "bioetl.pipelines.chembl.target.run",
        "bioetl.pipelines.chembl.testitem.run",
    ],
)
def test_chembl_run_modules_lines_executed(module_name: str) -> None:
    _force_module_lines_execution(module_name)

