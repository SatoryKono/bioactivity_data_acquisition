from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from pytest import MonkeyPatch
from typer.testing import CliRunner

from bioetl.cli.tools import catalog_code_symbols as catalog_cli


def test_catalog_code_symbols_success(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    runner: CliRunner,
) -> None:
    def fake_catalog_code_symbols(*, artifacts_dir: Path | None) -> SimpleNamespace:
        resolved = artifacts_dir or tmp_path
        return SimpleNamespace(json_path=resolved / "catalog.json", cli_path=resolved / "catalog.txt")

    monkeypatch.setattr(catalog_cli, "catalog_code_symbols", fake_catalog_code_symbols)

    artifacts_dir = tmp_path / "artifacts"
    result = runner.invoke(
        catalog_cli.app,
        ["--artifacts", str(artifacts_dir)],
    )

    assert result.exit_code == 0
    assert "Catalog updated" in result.stdout


def test_catalog_code_symbols_failure(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    runner: CliRunner,
) -> None:
    def fake_catalog_code_symbols(*, artifacts_dir: Path | None) -> SimpleNamespace:  # noqa: ARG001
        raise RuntimeError("catalog error")

    monkeypatch.setattr(catalog_cli, "catalog_code_symbols", fake_catalog_code_symbols)

    result = runner.invoke(
        catalog_cli.app,
        ["--artifacts", str(tmp_path / "artifacts")],
    )

    assert result.exit_code == 1
    assert "catalog error" in result.stderr


