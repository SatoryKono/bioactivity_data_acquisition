"""Shared pytest fixtures and configuration."""

import sys
from pathlib import Path

# Add src to path so imports work
project_root = Path(__file__).parent.parent
src_path = project_root / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

