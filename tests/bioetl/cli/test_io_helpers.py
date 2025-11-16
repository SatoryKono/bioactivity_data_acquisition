from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import hashlib
import yaml
import pytest

from bioetl.cli import _io


def test_atomic_write_yaml_invokes_fsync(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    target = tmp_path / "payload.yaml"
    payload = {"key": "value"}
    called: list[int] = []

    monkeypatch.setattr(_io.os, "fsync", lambda fd: called.append(fd))

    _io.atomic_write_yaml(payload, target)

    assert target.exists()
    assert yaml.safe_load(target.read_text(encoding="utf-8")) == payload
    assert called, "fsync should be invoked for durability"


def test_hash_file_blake2b(tmp_path: Path) -> None:
    blob = tmp_path / "blob.bin"
    blob.write_bytes(b"hello world")
    expected = hashlib.blake2b(b"hello world", digest_size=32).hexdigest()
    assert _io.hash_file(blob) == expected


def test_git_helpers_parse_output(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run(args: list[str], **kwargs: object) -> SimpleNamespace:
        if args[:2] == ["git", "ls-files"]:
            return SimpleNamespace(stdout="data/output/tracked.csv\n\n")
        if args[:4] == ["git", "diff", "--cached", "--name-only"]:
            return SimpleNamespace(stdout="data/output/new.csv\n")
        raise AssertionError(f"Unexpected command: {args}")

    monkeypatch.setattr(_io.subprocess, "run", fake_run)

    tracked = _io.git_ls("data/output")
    staged = _io.git_diff_cached("data/output")

    assert tracked == [Path("data/output/tracked.csv")]
    assert staged == [Path("data/output/new.csv")]
