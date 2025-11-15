"""Tests for filesystem helpers used by pipelines."""

from __future__ import annotations

from pathlib import Path

import pytest

from bioetl.pipelines.common import ensure_directory


@pytest.mark.unit
class TestEnsureDirectory:
    """Behavioural coverage for :func:`ensure_directory`."""

    def test_creates_missing_directory(self, tmp_path: Path) -> None:
        """A nested directory is created when it does not exist."""

        target = tmp_path / "nested" / "directory"

        result = ensure_directory(target)

        assert result == target
        assert target.exists()
        assert target.is_dir()

    def test_returns_existing_directory(self, tmp_path: Path) -> None:
        """Existing directories are returned unchanged."""

        target = tmp_path / "existing"
        target.mkdir()

        result = ensure_directory(target)

        assert result == target
        assert target.exists()
        assert target.is_dir()

    def test_raises_when_exist_ok_false(self, tmp_path: Path) -> None:
        """Attempting to recreate a directory with ``exist_ok=False`` fails."""

        target = tmp_path / "existing"
        target.mkdir()

        with pytest.raises(FileExistsError):
            ensure_directory(target, exist_ok=False)

    def test_raises_when_path_is_file(self, tmp_path: Path) -> None:
        """A file with the same name results in :class:`NotADirectoryError`."""

        target = tmp_path / "conflict"
        target.write_text("payload", encoding="utf-8")

        with pytest.raises(NotADirectoryError):
            ensure_directory(target)

    def test_handles_race_condition(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """If another process creates the directory first no error bubbles up."""

        target = tmp_path / "race" / "condition"
        original_mkdir = Path.mkdir
        call_counter = {"count": 0}

        def flaky_mkdir(
            self: Path,
            mode: int = 0o777,
            parents: bool = False,
            exist_ok: bool = False,
        ) -> None:  # type: ignore[override]
            if self == target and call_counter["count"] == 0:
                call_counter["count"] += 1
                original_mkdir(self, mode, parents=parents, exist_ok=True)
                raise FileExistsError("simulated race")
            original_mkdir(self, mode, parents=parents, exist_ok=exist_ok)

        monkeypatch.setattr(Path, "mkdir", flaky_mkdir)

        result = ensure_directory(target)

        assert result == target
        assert call_counter["count"] == 1
        assert target.exists()
        assert target.is_dir()
