from __future__ import annotations

from pathlib import Path

from bioetl.tools import get_project_root


def test_get_project_root_matches_module_root() -> None:
    import bioetl.tools as tools_module

    expected = Path(tools_module.__file__).resolve().parents[3]
    assert get_project_root() == expected
