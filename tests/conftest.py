"""Shared pytest fixtures for BioETL tests."""

from __future__ import annotations

import sys
from pathlib import Path

# Fix sys.path to prioritize current project over old one
# This ensures we import from bioactivity_data_acquisition2, not bioactivity_data_acquisition
_current_project_src = Path(__file__).parent.parent / "src"
_old_project_src = (
    Path(__file__).parent.parent.parent / "bioactivity_data_acquisition" / "src"
)
if str(_current_project_src) in sys.path and str(_old_project_src) in sys.path:
    if str(_old_project_src) in sys.path:
        sys.path.remove(str(_old_project_src))
    if str(_current_project_src) in sys.path:
        sys.path.remove(str(_current_project_src))
    sys.path.insert(0, str(_current_project_src))
elif str(_current_project_src) not in sys.path:
    sys.path.insert(0, str(_current_project_src))

pytest_plugins = [
    "tests.fixtures.paths",
    "tests.fixtures.data",
    "tests.fixtures.config",
    "tests.fixtures.mocks",
    "tests.fixtures.runtime",
]
