"""Deprecated compatibility layer for configuration models.

Prefer importing from `bioetl.config.models.models` (core models) or
`bioetl.config.models.policies` (policy objects). This package now simply
re-exports the canonical modules for backward compatibility.
"""

from __future__ import annotations

import warnings

from .models import *  # noqa: F401,F403
from .models import __all__ as _models_all
from .policies import *  # noqa: F401,F403
from .policies import __all__ as _policies_all

_EXPORTED_SYMBOLS = tuple(sorted(set(_models_all + _policies_all)))

__all__: tuple[str, ...] = _EXPORTED_SYMBOLS

warnings.warn(
    (
        "bioetl.config.models is deprecated; import from "
        "bioetl.config.models.models or bioetl.config.models.policies instead."
    ),
    DeprecationWarning,
    stacklevel=2,
)
