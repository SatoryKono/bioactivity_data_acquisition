from __future__ import annotations

import csv
from pathlib import Path

from bioetl.tools import dup_finder


def _write_module(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content.lstrip("\n"), encoding="utf-8")


def test_code_unit_normalization_removes_noise(tmp_path: Path) -> None:
    root = tmp_path
    module_path = root / "src" / "pkg" / "sample.py"
    _write_module(
        module_path,
        """
import logging


def sample(value, *, beta=2, alpha=1):
    \"\"\"Docstring describing the function.\"\"\"
    text = "hello world"
    number = 42
    logger = logging.getLogger(__name__)
    logger.info("value %s", text)
    data = {"b": 2, "a": 1}
    print("debug", beta)
    return target_call(beta=beta, alpha=alpha)
        """,
    )
    units, errors = dup_finder._parse_code_units(module_path, root)
    assert not errors
    assert len(units) == 1
    unit = units[0]
    assert "Docstring" not in unit.norm_src
    assert "'STR'" in unit.norm_src or '"STR"' in unit.norm_src
    assert "logger.info" not in unit.norm_src
    assert "print(" not in unit.norm_src
    assert unit.norm_src.count("'STR': 'NUM'") == 2
    alpha_index = unit.norm_src.index("alpha=alpha")
    beta_index = unit.norm_src.index("beta=beta")
    assert alpha_index < beta_index
    assert unit.tokens.count("NAME") > 0


def test_duplicate_clusters_and_near_duplicates(tmp_path: Path) -> None:
    root = tmp_path
    module_a = root / "src" / "pkg" / "module_a.py"
    module_b = root / "src" / "pkg" / "module_b.py"
    module_c = root / "src" / "pkg" / "module_c.py"
    _write_module(
        module_a,
        """
def foo(value):
    return value + 1
        """,
    )
    _write_module(
        module_b,
        """
def foo(value):
    return value + 1
        """,
    )
    _write_module(
        module_c,
        """
def compute_average(items):
    total = sum(items)
    return total / len(items)


def compute_ratio(elements):
    aggregate = sum(elements)
    return aggregate / len(elements)
        """,
    )
    units: list[dup_finder.CodeUnit] = []
    for module in (module_a, module_b, module_c):
        parsed, errors = dup_finder._parse_code_units(module, root)
        assert not errors
        units.extend(parsed)
    clusters = dup_finder._build_clusters(units)
    assert clusters
    duplicate = clusters[0]
    assert duplicate.ast_hash == duplicate.members[0].ast_hash == duplicate.members[1].ast_hash
    assert {member.rel_path.as_posix() for member in duplicate.members} == {
        "src/pkg/module_a.py",
        "src/pkg/module_b.py",
    }
    near_duplicates = dup_finder._build_near_duplicates(units)
    assert any(
        pair.unit_a.symbol == "compute_average"
        and pair.unit_b.symbol == "compute_ratio"
        for pair in near_duplicates
    )


def test_run_dup_finder_creates_reports(tmp_path: Path) -> None:
    root = tmp_path
    module = root / "src" / "pkg" / "report_sample.py"
    _write_module(
        module,
        """
def report_target(value):
    result = value * 2
    return result
        """,
    )
    output_dir = root / "reports"
    dup_finder.run_dup_finder(root, output_dir, formats=("csv", "md"))

    csv_path = output_dir / "dup_map.csv"
    md_path = output_dir / "dup_map.md"
    assert csv_path.exists()
    assert md_path.exists()
    with csv_path.open(encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
    assert rows and rows[0]["symbol"] == "report_target"
    assert rows[0]["path"] == "src/pkg/report_sample.py"
    assert rows[0]["ast_hash"]
    markdown = md_path.read_text(encoding="utf-8")
    assert "[src/pkg/report_sample.py#L1-L3]" in markdown
    assert "<pre><code>def report_target" in markdown

