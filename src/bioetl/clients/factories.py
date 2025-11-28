"""Legacy factories shim for backwards compatibility."""

from bioetl.clients.chembl.factories import *  # noqa: F401,F403

__all__ = [
    "default_chembl_factory",
    "make_chembl_client",
    "default_activity_client_factory",
]

