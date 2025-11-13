"""Activity-specific HTTP client helpers built on top of :mod:`bioetl.clients.client_chembl_common`."""

from __future__ import annotations

from typing import Any

from bioetl.clients.client_chembl_base import EntityConfig
from bioetl.clients.client_chembl_iterator import ChemblEntityIteratorBase

__all__ = ["ChemblActivityClient"]


class ChemblActivityClient(ChemblEntityIteratorBase):
    """High level helper focused on retrieving activity payloads."""

    def __init__(
        self,
        chembl_client: Any,  # ChemblClient
        *,
        batch_size: int = 25,
        max_url_length: int | None = None,
    ) -> None:
        """Инициализировать клиент для activity.

        Parameters
        ----------
        chembl_client:
            Экземпляр ChemblClient для выполнения запросов.
        batch_size:
            Размер батча для пагинации (максимум 25 для ChEMBL API).
        max_url_length:
            Максимальная длина URL для проверки. Если None, проверка отключена.
        """
        config = EntityConfig(
            endpoint="/activity.json",
            filter_param="activity_id__in",
            id_key="activity_id",
            items_key="activities",
            log_prefix="activity",
            chunk_size=100,
            supports_list_result=False,
            base_endpoint_length=len("/activity.json?"),
            enable_url_length_check=False,
        )

        super().__init__(
            chembl_client=chembl_client,
            config=config,
            batch_size=batch_size,
            max_url_length=max_url_length,
        )

