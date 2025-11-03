"""Document pipeline client utilities."""

from bioetl.core.deprecation import warn_legacy_client

from .document_client import DocumentChEMBLClient, DocumentFetchCallbacks

warn_legacy_client(__name__, replacement="bioetl.adapters.chembl.document")

__all__ = ["DocumentChEMBLClient", "DocumentFetchCallbacks"]
