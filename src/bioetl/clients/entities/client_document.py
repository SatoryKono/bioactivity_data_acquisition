"""Document-specific HTTP client helpers built on top of :mod:`bioetl.clients.client_chembl_common`."""

from __future__ import annotations

from typing import Any

from bioetl.clients.client_chembl_base import EntityConfig
from bioetl.clients.client_chembl_iterator import ChemblEntityIteratorBase

__all__ = ["ChemblDocumentClient"]


class ChemblDocumentClient(ChemblEntityIteratorBase):
    """High level helper focused on retrieving document payloads."""

    def __init__(
        self,
        chembl_client: Any,  # ChemblClient
        *,
        batch_size: int = 25,
        max_url_length: int | None = None,
    ) -> None:
        """Initialize the document client wrapper.

        Parameters
        ----------
        chembl_client:
            Instance of ChemblClient used to perform API calls.
        batch_size:
            Batch size for pagination (max 25 for the ChEMBL API).
        max_url_length:
            Optional URL length limit; disables the check when None.
        """
        config = EntityConfig(
            endpoint="/document.json",
            filter_param="document_chembl_id__in",
            id_key="document_chembl_id",
            items_key="documents",
            log_prefix="document",
            chunk_size=100,
            supports_list_result=False,
            base_endpoint_length=len("/document.json?"),
            enable_url_length_check=False,
        )

        super().__init__(
            chembl_client=chembl_client,
            config=config,
            batch_size=batch_size,
            max_url_length=max_url_length,
        )

