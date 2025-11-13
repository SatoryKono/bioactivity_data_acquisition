"""BioETL command interface with lazy initialization helpers."""

from __future__ import annotations

from importlib import import_module
from types import ModuleType
from typing import TYPE_CHECKING, Any, cast

__all__ = ["app", "create_app", "run"]

_CLI_APP_MODULE = "bioetl.cli.cli_app"

if TYPE_CHECKING:
    from bioetl.cli.cli_app import app as app  # type: ignore[attr-defined]
    from bioetl.cli.cli_app import create_app as create_app
    from bioetl.cli.cli_app import run as run


def _load_cli_app_module() -> ModuleType:
    """Lazily import ``cli_app`` to avoid circular dependencies."""

    module = import_module(_CLI_APP_MODULE)
    return cast(ModuleType, module)


def __getattr__(name: str) -> Any:
    """Expose CLI attributes through lazy loading on first access."""

    if name not in __all__:
        raise AttributeError(name)

    module = _load_cli_app_module()
    value = getattr(module, name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    """Return the dynamically available CLI attributes."""

    return sorted(set(__all__ + list(globals().keys())))