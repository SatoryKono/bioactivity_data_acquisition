from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from pytest import MonkeyPatch
from typer.testing import CliRunner

from bioetl.cli.tools import vocab_audit as vocab_audit_cli


def test_vocab_audit_success(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    runner: CliRunner,
) -> None:
    output_path = tmp_path / "audit.csv"
    meta_path = tmp_path / "meta.yaml"

    def fake_audit_vocabularies(
        *,
        store: Path | None,
        output: Path,
        meta: Path,
        pages: int,
        page_size: int,
    ) -> SimpleNamespace:
        assert pages == 10
        assert page_size == 1000
        return SimpleNamespace(rows=[{"id": 1}], output=output, meta=meta)

    monkeypatch.setattr(vocab_audit_cli, "audit_vocabularies", fake_audit_vocabularies)

    result = runner.invoke(
        vocab_audit_cli.app,
        [
            "--store",
            str(tmp_path),
            "--output",
            str(output_path),
            "--meta",
            str(meta_path),
        ],
    )

    assert result.exit_code == 0
    assert "Vocabulary audit completed" in result.stdout


def test_vocab_audit_failure(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    runner: CliRunner,
) -> None:
    def fake_audit_vocabularies(
        *,
        store: Path | None,
        output: Path,
        meta: Path,
        pages: int,
        page_size: int,
    ) -> SimpleNamespace:  # noqa: ARG001
        raise RuntimeError("vocab audit failed")

    monkeypatch.setattr(vocab_audit_cli, "audit_vocabularies", fake_audit_vocabularies)

    result = runner.invoke(
        vocab_audit_cli.app,
        [
            "--store",
            str(tmp_path),
            "--output",
            str(tmp_path / "audit.csv"),
            "--meta",
            str(tmp_path / "meta.yaml"),
        ],
    )

    assert result.exit_code == 1
    assert "vocab audit failed" in result.stderr


