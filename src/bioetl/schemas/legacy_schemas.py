"""Legacy schema module aliases preserved for backwards compatibility."""

from __future__ import annotations

import sys
from types import ModuleType

from . import chembl_activity_schema, chembl_target_schema

LegacyModuleMap = dict[str, ModuleType]

LEGACY_MODULE_ALIASES: LegacyModuleMap = {
    "bioetl.schemas.chembl_activity_schema": chembl_activity_schema,
    "bioetl.schemas.target_schema": chembl_target_schema,
}


def register_legacy_schema_modules() -> None:
    """Expose removed schema modules via ``sys.modules`` aliases."""

    for dotted_name, module in LEGACY_MODULE_ALIASES.items():
        sys.modules.setdefault(dotted_name, module)


__all__ = ["LEGACY_MODULE_ALIASES", "register_legacy_schema_modules"]

