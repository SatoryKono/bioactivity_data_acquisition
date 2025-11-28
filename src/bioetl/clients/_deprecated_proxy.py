from __future__ import annotations

import importlib
import warnings
from dataclasses import dataclass
from types import ModuleType
from typing import Any, Iterable


@dataclass(frozen=True)
class DeprecatedProxyConfig:
    """Configuration for creating a deprecated re-export proxy.

    Attributes:
        target: Dotted path to the module that contains the actual implementation.
        exports: Names to expose from the target module.
        message: Deprecation warning message to emit on first import. Set to
            ``None`` to disable warnings.
        warn_once: If ``True``, the warning is emitted only the first time the
            proxy is constructed.
    """

    target: str
    exports: tuple[str, ...]
    message: str | None
    warn_once: bool = True


_WARNED_CONFIGS: set[DeprecatedProxyConfig] = set()


def make_deprecated_proxy(
    config: DeprecatedProxyConfig, *, stacklevel: int = 2
) -> tuple[ModuleType, list[str]]:
    """Import the target module and optionally emit a deprecation warning.

    Returns the imported module and the list of exported attribute names.
    """

    should_warn = config.message is not None and (
        not config.warn_once or config not in _WARNED_CONFIGS
    )
    if should_warn:
        warnings.warn(config.message, DeprecationWarning, stacklevel=stacklevel)
        if config.warn_once:
            _WARNED_CONFIGS.add(config)

    module = importlib.import_module(config.target)
    return module, list(config.exports)


def export_from(module: ModuleType, names: Iterable[str]) -> dict[str, Any]:
    """Build a mapping of exported names from the target module."""

    return {name: getattr(module, name) for name in names}
