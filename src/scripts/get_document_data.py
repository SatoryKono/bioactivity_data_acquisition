"""Deprecated wrapper delegating to ``library.cli``."""

from __future__ import annotations

import sys
from pathlib import Path

if __package__ in (None, ""):
    SRC_PATH = Path(__file__).resolve().parents[1]
    if str(SRC_PATH) not in sys.path:
        sys.path.insert(0, str(SRC_PATH))

from library.cli import app as bioactivity_app, main as bioactivity_main  # type: ignore




def main() -> None:
    """Entry point maintained for backwards compatibility."""

   
    bioactivity_main()


def app(*args: object, **kwargs: object) -> object:
    """Proxy that preserves the historical ``app`` callable."""

    
    return bioactivity_app(*args, **kwargs)


if __name__ == "__main__":
    main()


__all__ = ["app", "main"]
