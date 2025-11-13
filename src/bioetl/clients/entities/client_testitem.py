"""Testitem-specific HTTP client helpers built on top of :mod:`bioetl.clients.client_chembl_common`."""

from __future__ import annotations

from typing import Any

from bioetl.clients.client_chembl_base import EntityConfig
from bioetl.clients.client_chembl_iterator import ChemblEntityIteratorBase

__all__ = ["ChemblTestitemClient"]


class ChemblTestitemClient(ChemblEntityIteratorBase):
    """High level helper focused on retrieving molecule (testitem) payloads."""

    def __init__(
        self,
        chembl_client: Any,  # ChemblClient
        *,
        batch_size: int = 25,
        max_url_length: int | None = None,
    ) -> None:
        """Initialize the molecule (test item) client wrapper.

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
            endpoint="/molecule.json",
            filter_param="molecule_chembl_id__in",
            id_key="molecule_chembl_id",
            items_key="molecules",
            log_prefix="molecule",
            chunk_size=100,
            supports_list_result=False,
            base_endpoint_length=len("/molecule.json?"),
            enable_url_length_check=False,
        )

        super().__init__(
            chembl_client=chembl_client,
            config=config,
            batch_size=batch_size,
            max_url_length=max_url_length,
        )

