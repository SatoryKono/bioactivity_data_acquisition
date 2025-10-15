"""Deprecated wrapper delegating to ``library.cli``."""

from __future__ import annotations

import sys
import warnings
from pathlib import Path

if __package__ in (None, ""):
    SRC_PATH = Path(__file__).resolve().parents[1] / "src"
    if str(SRC_PATH) not in sys.path:
        sys.path.insert(0, str(SRC_PATH))

from library import cli as bioactivity_cli  # type: ignore

_DEPRECATION_MESSAGE = (
    "scripts/get_document_data.py is deprecated and will be removed in a future release. "
    "Invoke `bioactivity-data-acquisition pipeline` instead."
)


def main() -> None:
    """Entry point maintained for backwards compatibility."""

    warnings.warn(_DEPRECATION_MESSAGE, DeprecationWarning, stacklevel=2)
    bioactivity_cli.main()


def app(*args: object, **kwargs: object) -> object:
    """Proxy that preserves the historical ``app`` callable."""

    warnings.warn(_DEPRECATION_MESSAGE, DeprecationWarning, stacklevel=2)
    return bioactivity_cli.app(*args, **kwargs)


if __name__ == "__main__":
    main()


__all__ = ["app", "main"]
