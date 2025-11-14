"""Registry hookup for built-in schema migrations."""

from __future__ import annotations

from importlib import import_module
from typing import Sequence

__all__ = ["load_builtin_migrations"]

BUILTIN_MIGRATION_MODULES: Sequence[str] = (
    "bioetl.schemas.migrations.chembl_activity",
    "bioetl.schemas.migrations.chembl_assay",
    "bioetl.schemas.migrations.chembl_document",
    "bioetl.schemas.migrations.chembl_target",
    "bioetl.schemas.migrations.chembl_testitem",
    "bioetl.schemas.migrations.chembl_metadata",
)


def _register_module(module_path: str) -> None:
    module = import_module(module_path)
    register = getattr(module, "register_migrations", None)
    if callable(register):
        register()


def load_builtin_migrations() -> None:
    """Import known migration modules so they can register themselves."""

    for module_path in BUILTIN_MIGRATION_MODULES:
        try:
            _register_module(module_path)
        except ModuleNotFoundError:
            # Allow missing modules to keep optional schemas optional.
            continue

