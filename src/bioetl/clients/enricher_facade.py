"""Совместимость: фасад обогащения перенесён в ``clients.factories``."""

from __future__ import annotations

import warnings

from bioetl.clients.factories.enricher_facade import *  # noqa: F401,F403

warnings.warn(
    "Импортируйте фасад обогащения из bioetl.clients.factories.enricher_facade; старый путь устарел.",
    DeprecationWarning,
    stacklevel=2,
)
