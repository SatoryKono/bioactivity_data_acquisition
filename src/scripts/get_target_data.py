"""Deprecated wrapper delegating to ``library.cli``."""

from __future__ import annotations

import os
import sys

# Setup path for library imports
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.dirname(current_dir)
# Always insert src_dir at the beginning to ensure it's found first
sys.path.insert(0, src_dir)

from library.scripts_base import create_deprecated_script_wrapper

main, app = create_deprecated_script_wrapper("get_target_data.py")


if __name__ == "__main__":
    main()


__all__ = ["app", "main"]
