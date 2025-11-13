from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

import pytest
import typer

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


def test_collect_python_files_missing_src(tmp_path: Path) -> None:
    python_files, warnings = dup_finder._collect_python_files(tmp_path)
    assert python_files == []
    assert warnings == [f"Directory {tmp_path / 'src'} is not accessible"]


def test_parse_code_units_reports_syntax_error(tmp_path: Path) -> None:
    file_path = tmp_path / "src" / "pkg" / "broken.py"
    _write_module(
        file_path,
        """
def invalid():
    if True
        return 1
""",
    )
    units, errors = dup_finder._parse_code_units(file_path, tmp_path)
    assert not units
    assert errors and "expected ':'" in errors[0].message


def test_render_to_stdout_outputs_markdown_and_csv(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    module_path = tmp_path / "src" / "pkg" / "report.py"
    _write_module(
        module_path,
        """
def sample(value):
    return value + 2
""",
    )
    units, errors = dup_finder._parse_code_units(module_path, tmp_path)
    assert not errors
    dup_finder._render_to_stdout(("csv", "md"), units, [], [], tests_present=False)
    captured = capsys.readouterr().out
    assert "# dup_map.csv" in captured
    assert "# dup_map.md" in captured
    assert "ARCHIVE_TESTS" in captured


def test_write_errors_and_warnings(tmp_path: Path) -> None:
    errors = [dup_finder.ParseError(path=Path("broken.py"), message="boom")]
    warnings = ["Directory missing"]
    errors_path = tmp_path / "out" / "errors.csv"
    warnings_path = tmp_path / "out" / "warnings.log"
    dup_finder._write_errors(errors_path, errors)
    dup_finder._write_warnings(warnings_path, warnings)
    with errors_path.open(encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        row = next(reader)
    assert row["path"] == "broken.py"
    assert warnings_path.read_text(encoding="utf-8").strip() == "Directory missing"


def test_main_rejects_invalid_format(tmp_path: Path) -> None:
    with pytest.raises(typer.BadParameter):
        dup_finder.main(root=tmp_path, out=tmp_path / "out", fmt="md,txt")


class _LoggerStub:
    def __init__(self) -> None:
        self.messages: list[tuple[Any, dict[str, Any]]] = []

    def error(self, event: Any, **kwargs: Any) -> None:
        self.messages.append((event, kwargs))


class _UnifiedLoggerStub:
    def __init__(self) -> None:
        self.logger = _LoggerStub()

    @staticmethod
    def configure() -> None:
        pass

    def get(self, _: str) -> _LoggerStub:
        return self.logger


def test_main_handles_runtime_errors(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    logger_stub = _UnifiedLoggerStub()
    monkeypatch.setattr(dup_finder, "UnifiedLogger", logger_stub)
    monkeypatch.setattr(dup_finder, "run_dup_finder", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("fail")))

    def fake_exit(code: int, *, cause: Exception | None = None) -> None:
        exit_exc = typer.Exit(code=code)
        setattr(exit_exc, "code", code)
        if cause is not None:
            raise exit_exc from cause
        raise exit_exc

    monkeypatch.setattr(dup_finder.CliCommandBase, "exit", staticmethod(fake_exit))

    with pytest.raises(typer.Exit) as excinfo:
        dup_finder.main(root=tmp_path, out=tmp_path / "out", fmt="md")

    assert getattr(excinfo.value, "code", None) == 1
    assert logger_stub.logger.messages
    event, payload = logger_stub.logger.messages[0]
    assert event == dup_finder.LogEvents.DUP_FINDER_FAILED
    assert payload["exception_type"] == "RuntimeError"


def test_classify_role_variants() -> None:
    assert dup_finder._classify_role(Path("bioetl/clients/api.py"), "fetch") == "client"
    assert dup_finder._classify_role(Path("bioetl/config/model.py"), "make") == "schema"
    assert dup_finder._classify_role(Path("bioetl/cli/main.py"), "command") == "cli"
    assert dup_finder._classify_role(Path("bioetl/utils/logger.py"), "log_event") == "log"
    assert dup_finder._classify_role(Path("bioetl/pipelines/foo/run.py"), "process") == "run"
    assert dup_finder._classify_role(Path("bioetl/pipelines/foo/module.py"), "extract_dataset") == "extract"
    assert dup_finder._classify_role(Path("bioetl/pipelines/foo/module.py"), "transform_dataset") == "transform"
    assert dup_finder._classify_role(Path("bioetl/pipelines/foo/module.py"), "validate_dataset") == "validate"
    assert dup_finder._classify_role(Path("bioetl/pipelines/foo/module.py"), "write_output") == "write"
    assert dup_finder._classify_role(Path("bioetl/utils/misc.py"), "helper") == "util"


def test_write_markdown_with_clusters_and_pairs(tmp_path: Path) -> None:
    base_path = tmp_path / "src" / "pkg" / "module.py"
    unit_a = dup_finder.CodeUnit(
        symbol="foo",
        kind="func",
        role="util",
        path=base_path,
        rel_path=Path("src/pkg/module.py"),
        start_line=1,
        end_line=3,
        norm_src="def foo():\n    return 1",
        norm_loc=2,
        ast_hash="hash1",
        tokens=("def", "foo"),
        token_multiset=dup_finder.Counter({"def": 1, "foo": 1}),
        snippet="def foo():\n    return 1",
    )
    unit_b = dup_finder.CodeUnit(
        symbol="foo_copy",
        kind="func",
        role="util",
        path=base_path,
        rel_path=Path("src/pkg/module_copy.py"),
        start_line=1,
        end_line=3,
        norm_src="def foo_copy():\n    return 1",
        norm_loc=2,
        ast_hash="hash1",
        tokens=("def", "foo_copy"),
        token_multiset=dup_finder.Counter({"def": 1, "foo_copy": 1}),
        snippet="def foo_copy():\n    return 1",
    )
    cluster = dup_finder.DuplicateCluster(ast_hash="hash1", members=(unit_a, unit_b))
    pair = dup_finder.NearDuplicatePair(
        unit_a=unit_a,
        unit_b=unit_b,
        jaccard=0.95,
        lcs_ratio=0.97,
        divergences="equal",
    )
    markdown_path = tmp_path / "out.md"
    dup_finder._write_markdown(markdown_path, [unit_a], [cluster], [pair], tests_present=True)
    content = markdown_path.read_text(encoding="utf-8")
    assert "Duplicate clusters" in content
    assert "Near-duplicates" in content
    assert "foo_copy" in content


def test_render_to_stdout_with_clusters_and_pairs(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    unit_a = dup_finder.CodeUnit(
       symbol="Logger.log_event",
       kind="method",
       role="log",
       path=tmp_path / "src" / "pkg" / "module.py",
       rel_path=Path("src/pkg/module.py"),
       start_line=10,
       end_line=12,
       norm_src="def log_event():\n    return True",
       norm_loc=2,
       ast_hash="abc123",
       tokens=("def", "log_event"),
       token_multiset=dup_finder.Counter({"def": 1, "log_event": 1}),
       snippet="def log_event():\n    return True",
    )
    unit_b = dup_finder.CodeUnit(
       symbol="Logger.log_event_copy",
       kind="method",
       role="log",
       path=tmp_path / "src" / "pkg" / "module_copy.py",
       rel_path=Path("src/pkg/module_copy.py"),
       start_line=5,
       end_line=7,
       norm_src="def log_event_copy():\n    value = 1\n    return value",
       norm_loc=3,
       ast_hash="def456",
       tokens=("def", "log_event_copy"),
       token_multiset=dup_finder.Counter({"def": 1, "log_event_copy": 1}),
       snippet="def log_event_copy():\n    return True",
    )
    cluster = dup_finder.DuplicateCluster(ast_hash="cluster123", members=(unit_a, unit_a))
    pair = dup_finder.NearDuplicatePair(
        unit_a=unit_a,
        unit_b=unit_b,
        jaccard=0.9,
        lcs_ratio=0.91,
        divergences="token_delta:1",
    )
    dup_finder._render_to_stdout(("md",), [unit_a], [cluster], [pair], tests_present=True)
    output = capsys.readouterr().out
    assert "dup_map.md" in output
    assert "cluster123" in output
    assert "token_delta:1" in output


def test_parse_code_units_handles_class_and_async(tmp_path: Path) -> None:
    module_path = tmp_path / "src" / "pkg" / "complex.py"
    _write_module(
        module_path,
        """
class Handler:
    \"\"\"Docstring.\"\"\"

    async def load(self):
        return await process()

async def top_level():
    return 42
""",
    )
    units, errors = dup_finder._parse_code_units(module_path, tmp_path)
    assert not errors
    kinds = {unit.kind for unit in units}
    assert kinds == {"class", "method", "func"}


def test_collect_python_files_permission_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    src_dir = tmp_path / "src"
    (src_dir / "pkg").mkdir(parents=True)

    class RaisingList(list):
        def __iter__(self):
            raise PermissionError

    def fake_walk(_: Path):
        yield (str(src_dir / "pkg"), [], RaisingList(["sample.py"]))

    monkeypatch.setattr(dup_finder.os, "walk", fake_walk)
    paths, warnings = dup_finder._collect_python_files(tmp_path)
    assert paths == []
    assert any("pkg" in warning for warning in warnings)


def test_parse_code_units_reports_os_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    file_path = tmp_path / "src" / "pkg" / "missing.py"
    file_path.parent.mkdir(parents=True)

    def fake_read_text(self: Path, encoding: str = "utf-8") -> str:  # noqa: ARG001
        raise OSError("boom")

    monkeypatch.setattr(Path, "read_text", fake_read_text)
    units, errors = dup_finder._parse_code_units(file_path, tmp_path)
    assert not units
    assert errors and errors[0].message == "boom"

