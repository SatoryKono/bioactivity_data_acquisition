"""Base functionality for deprecated script wrappers."""

from __future__ import annotations

import sys
import warnings
from collections.abc import Callable
from pathlib import Path

from library.cli import app as bioactivity_app
from library.cli import main as bioactivity_main


class DeprecatedScriptWrapper:
    """Base class for deprecated script wrappers that delegate to library.cli."""

    def __init__(self, script_name: str) -> None:
        """Initialize the wrapper with the script name for deprecation messages."""
        self.script_name = script_name
        self._deprecation_message = f"scripts/{script_name} is deprecated and will be removed in a future release. Invoke `bioactivity-data-acquisition pipeline` instead."

    def setup_path(self) -> None:
        """Setup the Python path to include the src directory."""
        if __package__ in (None, ""):
            SRC_PATH = Path(__file__).resolve().parents[1]
            if str(SRC_PATH) not in sys.path:
                sys.path.insert(0, str(SRC_PATH))

    def main(self) -> None:
        """Entry point maintained for backwards compatibility."""
        warnings.warn(self._deprecation_message, DeprecationWarning, stacklevel=2)
        bioactivity_main()

    def app(self, *args: object, **kwargs: object) -> object:
        """Proxy that preserves the historical ``app`` callable."""
        warnings.warn(self._deprecation_message, DeprecationWarning, stacklevel=2)
        return bioactivity_app(*args, **kwargs)


def create_deprecated_script_wrapper(
    script_name: str,
) -> tuple[Callable[[], None], Callable[..., object]]:
    """Create main and app functions for a deprecated script wrapper."""
    wrapper = DeprecatedScriptWrapper(script_name)
    wrapper.setup_path()

    def main() -> None:
        return wrapper.main()

    def app(*args: object, **kwargs: object) -> object:
        return wrapper.app(*args, **kwargs)

    return main, app


__all__ = ["DeprecatedScriptWrapper", "create_deprecated_script_wrapper"]
