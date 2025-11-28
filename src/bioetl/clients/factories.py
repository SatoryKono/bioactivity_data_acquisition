"""Legacy factories shim for backwards compatibility."""

from bioetl.clients._deprecated_proxy import (
    DeprecatedProxyConfig,
    export_from,
    make_deprecated_proxy,
)
from bioetl.clients.chembl.factories import (
    FACTORIES_DEPRECATION_MESSAGE,
    FACTORIES_EXPORTS,
)


_PROXY_CONFIG = DeprecatedProxyConfig(
    target="bioetl.clients.chembl._factories_impl",
    exports=FACTORIES_EXPORTS,
    message=FACTORIES_DEPRECATION_MESSAGE,
)

_module, __all__ = make_deprecated_proxy(_PROXY_CONFIG, stacklevel=3)
globals().update(export_from(_module, __all__))

__all__ = list(__all__)
