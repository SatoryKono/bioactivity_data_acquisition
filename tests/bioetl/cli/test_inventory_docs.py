from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from pytest import MonkeyPatch
from typer.testing import CliRunner

from bioetl.cli.tools import inventory_docs as inventory_cli


def test_inventory_docs_success(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    runner: CliRunner,
) -> None:
    inventory_path = tmp_path / "inventory.txt"
    hashes_path = tmp_path / "hashes.txt"

    def fake_write_inventory(*, inventory_path: Path, hashes_path: Path) -> SimpleNamespace:
        return SimpleNamespace(
            files=["README.md"],
            inventory_path=inventory_path,
            hashes_path=hashes_path,
        )

    monkeypatch.setattr(inventory_cli, "write_inventory", fake_write_inventory)

    result = runner.invoke(
        inventory_cli.app,
        [
            "--inventory",
            str(inventory_path),
            "--hashes",
            str(hashes_path),
        ],
    )

    assert result.exit_code == 0
    assert "Inventory completed" in result.stdout


def test_inventory_docs_failure(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    runner: CliRunner,
) -> None:
    def fake_write_inventory(*, inventory_path: Path, hashes_path: Path) -> SimpleNamespace:  # noqa: ARG001
        raise RuntimeError("inventory failure")

    monkeypatch.setattr(inventory_cli, "write_inventory", fake_write_inventory)

    result = runner.invoke(
        inventory_cli.app,
        [
            "--inventory",
            str(tmp_path / "inventory.txt"),
            "--hashes",
            str(tmp_path / "hashes.txt"),
        ],
    )

    assert result.exit_code == 1
    assert "inventory failure" in result.stderr


