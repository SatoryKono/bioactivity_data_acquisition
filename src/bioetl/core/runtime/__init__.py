"""Runtime primitives and compatibility helpers for BioETL core."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any, Mapping

from .cli_base import CliCommandBase, CliEntrypoint
from .errors import BioETLError

if TYPE_CHECKING:
    from .load_meta_store import LoadMetaStore


_LAZY_EXPORTS: Mapping[str, str] = {
    "LoadMetaStore": "bioetl.core.runtime.load_meta_store",
    "RunArtifacts": "bioetl.core.io",
    "WriteArtifacts": "bioetl.core.io",
    "WriteResult": "bioetl.core.io",
}


def __getattr__(name: str) -> Any:
    module_name = _LAZY_EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(name)
    module = import_module(module_name)
    return getattr(module, name)


__all__ = [
    "BioETLError",
    "CliCommandBase",
    "CliEntrypoint",
    *sorted(_LAZY_EXPORTS),
]

