"""Совместимость: фабрика обогащающих клиентов перенесена в ``clients.factories``."""

from __future__ import annotations

import warnings

from bioetl.clients.factories.enricher_factory import *  # noqa: F401,F403

warnings.warn(
    "Импортируйте фабрику из bioetl.clients.factories.enricher_factory; старый путь устарел.",
    DeprecationWarning,
    stacklevel=2,
)
