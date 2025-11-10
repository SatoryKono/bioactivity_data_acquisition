"""Shared base implementation for high level ChEMBL entity clients."""

from __future__ import annotations

from abc import ABC, abstractmethod

from bioetl.clients.chembl_base import ChemblClientProtocol, EntityConfig
from bioetl.clients.chembl_iterator import ChemblEntityIterator

__all__ = ["ChemblEntityClientBase"]


class ChemblEntityClientBase(ChemblEntityIterator, ABC):
    """Base class encapsulating common options for entity clients."""

    DEFAULT_BATCH_SIZE: int = 25
    DEFAULT_MAX_URL_LENGTH: int | None = None

    def __init__(
        self,
        chembl_client: ChemblClientProtocol,
        *,
        batch_size: int | None = None,
        max_url_length: int | None = None,
    ) -> None:
        """Initialise the entity client with shared pagination options."""

        resolved_batch_size = (
            batch_size if batch_size is not None else self.DEFAULT_BATCH_SIZE
        )
        resolved_max_url_length = (
            max_url_length
            if max_url_length is not None
            else self.DEFAULT_MAX_URL_LENGTH
        )

        super().__init__(
            chembl_client=chembl_client,
            config=self._create_config(resolved_max_url_length),
            batch_size=resolved_batch_size,
            max_url_length=resolved_max_url_length,
        )

    @classmethod
    @abstractmethod
    def _create_config(cls, max_url_length: int | None) -> EntityConfig:
        """Construct the entity configuration for a concrete client."""

