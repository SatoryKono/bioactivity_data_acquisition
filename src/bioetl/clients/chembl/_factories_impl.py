from __future__ import annotations

import warnings

from bioetl.clients.chembl.factories import (
    FACTORIES_DEPRECATION_MESSAGE,
    default_activity_client_factory,
    default_chembl_factory,
    make_chembl_client,
)


warnings.warn(FACTORIES_DEPRECATION_MESSAGE, DeprecationWarning, stacklevel=2)

__all__ = (
    "default_chembl_factory",
    "make_chembl_client",
    "default_activity_client_factory",
    "FACTORIES_DEPRECATION_MESSAGE",
)
