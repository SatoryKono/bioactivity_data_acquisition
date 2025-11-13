from __future__ import annotations

from importlib import import_module
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import typer


def _invoke_main(module_path: str, *, kwargs: dict[str, Any] | None = None) -> typer.Exit:
    module = import_module(module_path)
    kwargs = kwargs or {}
    with pytest.raises(typer.Exit) as exit_info:
        module.main(**kwargs)
    return exit_info.value


def test_check_comments_variants(monkeypatch: pytest.MonkeyPatch) -> None:
    module = import_module("bioetl.cli.tools.check_comments")
    monkeypatch.setattr(module, "run_comment_check", lambda root=None: None)
    exit_info = _invoke_main("bioetl.cli.tools.check_comments", kwargs={"root": None})
    assert exit_info.code == 0

    monkeypatch.setattr(
        module,
        "run_comment_check",
        lambda root=None: (_ for _ in ()).throw(NotImplementedError("WIP")),
    )
    exit_info = _invoke_main("bioetl.cli.tools.check_comments", kwargs={"root": None})
    assert exit_info.code == 1

    monkeypatch.setattr(
        module,
        "run_comment_check",
        lambda root=None: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    exit_info = _invoke_main("bioetl.cli.tools.check_comments", kwargs={"root": None})
    assert exit_info.code == 1


def test_check_output_artifacts(monkeypatch: pytest.MonkeyPatch) -> None:
    module = import_module("bioetl.cli.tools.check_output_artifacts")
    monkeypatch.setattr(module, "check_output_artifacts", lambda max_bytes=0: [])
    exit_info = _invoke_main("bioetl.cli.tools.check_output_artifacts", kwargs={"max_bytes": 1})
    assert exit_info.code == 0

    monkeypatch.setattr(module, "check_output_artifacts", lambda max_bytes=0: ["err1", "err2"])
    exit_info = _invoke_main("bioetl.cli.tools.check_output_artifacts", kwargs={"max_bytes": 1})
    assert exit_info.code == 1

    monkeypatch.setattr(
        module,
        "check_output_artifacts",
        lambda max_bytes=0: (_ for _ in ()).throw(RuntimeError("failed")),
    )
    exit_info = _invoke_main("bioetl.cli.tools.check_output_artifacts", kwargs={"max_bytes": 1})
    assert exit_info.code == 1


def test_schema_guard(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    module = import_module("bioetl.cli.tools.schema_guard")
    success_report = tmp_path / "report"
    monkeypatch.setattr(
        module,
        "run_schema_guard",
        lambda: (
            {"pipeline": {"valid": True}},
            [],
            success_report,
        ),
    )
    exit_info = _invoke_main("bioetl.cli.tools.schema_guard")
    assert exit_info.code == 0

    monkeypatch.setattr(
        module,
        "run_schema_guard",
        lambda: (
            {"pipeline": {"valid": False}},
            ["registry error"],
            success_report,
        ),
    )
    exit_info = _invoke_main("bioetl.cli.tools.schema_guard")
    assert exit_info.code == 1

    monkeypatch.setattr(
        module,
        "run_schema_guard",
        lambda: (_ for _ in ()).throw(RuntimeError("check failed")),
    )
    exit_info = _invoke_main("bioetl.cli.tools.schema_guard")
    assert exit_info.code == 1


def test_link_check(monkeypatch: pytest.MonkeyPatch) -> None:
    module = import_module("bioetl.cli.tools.link_check")
    monkeypatch.setattr(module, "run_link_check", lambda timeout_seconds=0: 0)
    exit_info = _invoke_main("bioetl.cli.tools.link_check", kwargs={"timeout_seconds": 1})
    assert exit_info.code == 0

    monkeypatch.setattr(module, "run_link_check", lambda timeout_seconds=0: 2)
    exit_info = _invoke_main("bioetl.cli.tools.link_check", kwargs={"timeout_seconds": 1})
    assert exit_info.code == 2

    monkeypatch.setattr(
        module,
        "run_link_check",
        lambda timeout_seconds=0: (_ for _ in ()).throw(RuntimeError("lychee failed")),
    )
    exit_info = _invoke_main("bioetl.cli.tools.link_check", kwargs={"timeout_seconds": 1})
    assert exit_info.code == 1


def test_determinism_check(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    module = import_module("bioetl.cli.tools.determinism_check")
    report_path = tmp_path / "determinism-report"
    deterministic_result = SimpleNamespace(deterministic=True, report_path=report_path)
    monkeypatch.setattr(
        module,
        "run_determinism_check",
        lambda pipelines=None: {"pipe": deterministic_result},
    )
    exit_info = _invoke_main("bioetl.cli.tools.determinism_check", kwargs={"pipeline": ["pipe"]})
    assert exit_info.code == 0

    non_det_result = SimpleNamespace(deterministic=False, report_path=report_path)
    monkeypatch.setattr(
        module,
        "run_determinism_check",
        lambda pipelines=None: {"pipe": non_det_result},
    )
    exit_info = _invoke_main("bioetl.cli.tools.determinism_check", kwargs={"pipeline": ["pipe"]})
    assert exit_info.code == 1

    monkeypatch.setattr(module, "run_determinism_check", lambda pipelines=None: {})
    exit_info = _invoke_main("bioetl.cli.tools.determinism_check", kwargs={"pipeline": ["pipe"]})
    assert exit_info.code == 1

    monkeypatch.setattr(
        module,
        "run_determinism_check",
        lambda pipelines=None: (_ for _ in ()).throw(RuntimeError("determinism failed")),
    )
    exit_info = _invoke_main("bioetl.cli.tools.determinism_check", kwargs={"pipeline": ["pipe"]})
    assert exit_info.code == 1


def test_audit_docs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    module = import_module("bioetl.cli.tools.audit_docs")
    called: dict[str, Path] = {}
    monkeypatch.setattr(module, "run_audit", lambda artifacts_dir: called.setdefault("dir", artifacts_dir))
    artifacts_path = tmp_path / "artifacts"
    exit_info = _invoke_main("bioetl.cli.tools.audit_docs", kwargs={"artifacts": artifacts_path})
    assert exit_info.code == 0
    assert called["dir"] == artifacts_path.resolve()


def test_catalog_code_symbols(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    module = import_module("bioetl.cli.tools.catalog_code_symbols")
    result = SimpleNamespace(
        json_path=(tmp_path / "catalog.json"),
        cli_path=(tmp_path / "catalog.md"),
    )
    monkeypatch.setattr(module, "catalog_code_symbols", lambda artifacts_dir=None: result)
    exit_info = _invoke_main("bioetl.cli.tools.catalog_code_symbols", kwargs={"artifacts": None})
    assert exit_info.code == 0

    for exc_type in (module.BioETLError, module.CircuitBreakerOpenError, module.HTTPError, module.Timeout):
        monkeypatch.setattr(
            module,
            "catalog_code_symbols",
            lambda artifacts_dir=None, exc=exc_type("boom"): (_ for _ in ()).throw(exc),
        )
        exit_info = _invoke_main("bioetl.cli.tools.catalog_code_symbols", kwargs={"artifacts": None})
        assert exit_info.code == 1


def test_create_matrix_doc_code(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    module = import_module("bioetl.cli.tools.create_matrix_doc_code")
    matrix_result = SimpleNamespace(
        rows=[1, 2, 3],
        csv_path=(tmp_path / "matrix.csv"),
        json_path=(tmp_path / "matrix.json"),
    )
    monkeypatch.setattr(module, "write_matrix", lambda artifacts_dir: matrix_result)
    artifacts = tmp_path / "artifacts"
    exit_info = _invoke_main("bioetl.cli.tools.create_matrix_doc_code", kwargs={"artifacts": artifacts})
    assert exit_info.code == 0

    monkeypatch.setattr(
        module,
        "write_matrix",
        lambda artifacts_dir: (_ for _ in ()).throw(module.BioETLError("fail")),
    )
    exit_info = _invoke_main("bioetl.cli.tools.create_matrix_doc_code", kwargs={"artifacts": artifacts})
    assert exit_info.code == 1


def test_doctest_cli(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    module = import_module("bioetl.cli.tools.doctest_cli")
    report_path = tmp_path / "doctest-report"
    success_result = [
        SimpleNamespace(exit_code=0),
        SimpleNamespace(exit_code=0),
    ]
    monkeypatch.setattr(module, "run_examples", lambda: (success_result, report_path))
    exit_info = _invoke_main("bioetl.cli.tools.doctest_cli")
    assert exit_info.code == 0

    failed_result = [
        SimpleNamespace(exit_code=0),
        SimpleNamespace(exit_code=1),
    ]
    monkeypatch.setattr(module, "run_examples", lambda: (failed_result, report_path))
    exit_info = _invoke_main("bioetl.cli.tools.doctest_cli")
    assert exit_info.code == 1

    monkeypatch.setattr(
        module,
        "run_examples",
        lambda: (_ for _ in ()).throw(RuntimeError("doctest failed")),
    )
    exit_info = _invoke_main("bioetl.cli.tools.doctest_cli")
    assert exit_info.code == 1


def test_run_test_report(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    module = import_module("bioetl.cli.tools.run_test_report")
    monkeypatch.setattr(module, "generate_test_report", lambda output_root: 0)
    exit_info = _invoke_main(
        "bioetl.cli.tools.run_test_report",
        kwargs={"output_root": tmp_path / "reports"},
    )
    assert exit_info.code == 0

    monkeypatch.setattr(module, "generate_test_report", lambda output_root: 2)
    exit_info = _invoke_main(
        "bioetl.cli.tools.run_test_report",
        kwargs={"output_root": tmp_path / "reports"},
    )
    assert exit_info.code == 2

    monkeypatch.setattr(
        module,
        "generate_test_report",
        lambda output_root: (_ for _ in ()).throw(RuntimeError("pytest failed")),
    )
    exit_info = _invoke_main(
        "bioetl.cli.tools.run_test_report",
        kwargs={"output_root": tmp_path / "reports"},
    )
    assert exit_info.code == 1


def test_vocab_audit(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    module = import_module("bioetl.cli.tools.vocab_audit")
    result = SimpleNamespace(
        rows=[1, 2],
        output=tmp_path / "audit.csv",
        meta=tmp_path / "meta.yaml",
    )
    monkeypatch.setattr(module, "audit_vocabularies", lambda **kwargs: result)
    exit_info = _invoke_main(
        "bioetl.cli.tools.vocab_audit",
        kwargs={
            "store": None,
            "output": tmp_path / "audit.csv",
            "meta": tmp_path / "meta.yaml",
            "pages": 1,
            "page_size": 10,
        },
    )
    assert exit_info.code == 0

    monkeypatch.setattr(
        module,
        "audit_vocabularies",
        lambda **kwargs: (_ for _ in ()).throw(module.BioETLError("audit failed")),
    )
    exit_info = _invoke_main(
        "bioetl.cli.tools.vocab_audit",
        kwargs={
            "store": None,
            "output": tmp_path / "audit.csv",
            "meta": tmp_path / "meta.yaml",
            "pages": 1,
            "page_size": 10,
        },
    )
    assert exit_info.code == 1

