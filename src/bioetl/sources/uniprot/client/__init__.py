"""HTTP clients specialized for UniProt REST endpoints."""

from bioetl.core.deprecation import warn_legacy_client

from .idmapping_client import UniProtIdMappingClient
from .orthologs_client import UniProtOrthologsClient

# Import UniProtSearchClient from parent module client.py, not from search_client.py
# The client.py version has the correct API (client, fields, batch_size, fetch_entries)
# while search_client.py has different API (api, fetch)
try:
    # Import from parent module using relative import with different name to avoid conflict
    # This imports from ../client.py, not from ./ (current package)
    import sys
    import types

    # Create a new module name to avoid conflict with package 'client'
    _parent_module_name = "bioetl.sources.uniprot._client_py"

    # Use importlib to directly load client.py
    import importlib.util
    from pathlib import Path

    current_file = Path(__file__).resolve()
    client_py_path = current_file.parent.parent / "client.py"

    if client_py_path.exists() and client_py_path.is_file():
        spec = importlib.util.spec_from_file_location(_parent_module_name, client_py_path)
        if spec and spec.loader:
            _client_module = importlib.util.module_from_spec(spec)
            # Execute the module to load its contents
            spec.loader.exec_module(_client_module)
            if hasattr(_client_module, "UniProtSearchClient"):
                UniProtSearchClient = _client_module.UniProtSearchClient
            else:
                raise AttributeError("UniProtSearchClient not found in client.py")
        else:
            raise ImportError("Could not create module spec for client.py")
    else:
        raise FileNotFoundError(f"client.py not found at {client_py_path}")
except Exception as e:
    # Fallback to search_client.py if import fails
    from .search_client import UniProtSearchClient

warn_legacy_client(__name__, replacement="bioetl.adapters.uniprot")

# Backwards compatibility alias for legacy import paths
UniProtOrthologClient = UniProtOrthologsClient

__all__ = [
    "UniProtSearchClient",
    "UniProtIdMappingClient",
    "UniProtOrthologsClient",
    "UniProtOrthologClient",
]
