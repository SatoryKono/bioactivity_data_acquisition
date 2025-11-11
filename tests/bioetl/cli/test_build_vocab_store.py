from __future__ import annotations

from pathlib import Path
from typing import Protocol

import pytest
from typer.testing import CliRunner  # pyright: ignore[reportMissingImports]

from bioetl.cli.tools import build_vocab_store as build_vocab_store_cli


class MonkeyPatchProtocol(Protocol):
    def setattr(
        self,
        target: object,
        name: str | None,
        value: object,
        raising: bool = ...,
    ) -> None:
        ...


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


def test_build_vocab_store_success(
    tmp_path: Path,
    monkeypatch: MonkeyPatchProtocol,
    runner: CliRunner,
) -> None:
    captured: dict[str, Path] = {}

    def fake_build_vocab_store(*, src: Path, output: Path) -> Path:
        captured["src"] = src
        captured["output"] = output
        return output

    monkeypatch.setattr(build_vocab_store_cli, "build_vocab_store", fake_build_vocab_store)

    output_path = tmp_path / "out.yaml"
    result = runner.invoke(
        build_vocab_store_cli.app,
        ["--src", str(tmp_path), "--output", str(output_path)],
    )

    assert result.exit_code == 0
    assert "Агрегированный словарь записан" in result.stdout
    assert captured["src"] == tmp_path.resolve()
    assert captured["output"] == output_path.resolve()


def test_build_vocab_store_failure(
    tmp_path: Path,
    monkeypatch: MonkeyPatchProtocol,
    runner: CliRunner,
) -> None:
    def fake_build_vocab_store(*, src: Path, output: Path) -> Path:  # noqa: ARG001
        raise RuntimeError("build failed")

    monkeypatch.setattr(build_vocab_store_cli, "build_vocab_store", fake_build_vocab_store)

    result = runner.invoke(
        build_vocab_store_cli.app,
        ["--src", str(tmp_path), "--output", str(tmp_path / "out.yaml")],
    )

    assert result.exit_code == 1
    assert "build failed" in result.stderr


