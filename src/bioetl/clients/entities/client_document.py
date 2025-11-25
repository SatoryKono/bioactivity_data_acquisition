from __future__ import annotations

from bioetl.clients.entities._base import _BaseEntityClient
from bioetl.core.http.api_client import UnifiedAPIClient


class ChemblDocumentClient(_BaseEntityClient):
    def __init__(self, api_client: UnifiedAPIClient):
        super().__init__(api_client, "document")
