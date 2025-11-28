"""Factories for constructing ChEMBL clients."""

from bioetl.clients._deprecated_proxy import (
    DeprecatedProxyConfig,
    export_from,
    make_deprecated_proxy,
)
from bioetl.clients.chembl._factories_impl import (
    FACTORIES_DEPRECATION_MESSAGE as _FACTORIES_DEPRECATION_MESSAGE,
    FACTORIES_EXPORTS,
)


_PROXY_CONFIG = DeprecatedProxyConfig(
    target="bioetl.clients.chembl._factories_impl",
    exports=FACTORIES_EXPORTS,
    message=None,
)

_module, __all__ = make_deprecated_proxy(_PROXY_CONFIG, stacklevel=3)
globals().update(export_from(_module, __all__))

__all__ = list(__all__)
FACTORIES_DEPRECATION_MESSAGE = _FACTORIES_DEPRECATION_MESSAGE
