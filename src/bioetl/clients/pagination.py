"""Deprecated pagination shim for backwards compatibility."""

from bioetl.clients._deprecated_proxy import (
    DeprecatedProxyConfig,
    export_from,
    make_deprecated_proxy,
)
from bioetl.clients.chembl.pagination import (
    PAGINATION_DEPRECATION_MESSAGE,
    PAGINATION_EXPORTS,
)


_PROXY_CONFIG = DeprecatedProxyConfig(
    target="bioetl.clients.chembl._pagination_impl",
    exports=PAGINATION_EXPORTS,
    message=PAGINATION_DEPRECATION_MESSAGE,
)

_module, __all__ = make_deprecated_proxy(_PROXY_CONFIG, stacklevel=3)
globals().update(export_from(_module, __all__))

__all__ = list(__all__)
