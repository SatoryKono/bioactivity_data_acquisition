"""Target-specific HTTP client helpers built on top of :mod:`bioetl.clients.chembl`."""

from __future__ import annotations

from typing import Any, TypedDict

from bioetl.clients.chembl_base import EntityConfig
from bioetl.clients.chembl_iterator import ChemblEntityIterator


class _TargetEntityConfigDict(TypedDict, total=False):
    endpoint: str
    filter_param: str
    id_key: str
    items_key: str
    log_prefix: str
    chunk_size: int
    supports_list_result: bool
    base_endpoint_length: int
    enable_url_length_check: bool

__all__ = ["ChemblTargetClient"]


class ChemblTargetClient(ChemblEntityIterator):
    """High level helper focused on retrieving target payloads."""

    def __init__(
        self,
        chembl_client: Any,  # ChemblClient
        *,
        batch_size: int = 25,
        max_url_length: int | None = None,
    ) -> None:
        """Инициализировать клиент для target.

        Parameters
        ----------
        chembl_client:
            Экземпляр ChemblClient для выполнения запросов.
        batch_size:
            Размер батча для пагинации (максимум 25 для ChEMBL API).
        max_url_length:
            Максимальная длина URL для проверки. Если None, проверка отключена.
        """
        config_dict: _TargetEntityConfigDict = {
            "endpoint": "/target.json",
            "filter_param": "target_chembl_id__in",
            "id_key": "target_chembl_id",
            "items_key": "targets",
            "log_prefix": "target",
            "chunk_size": 100,
            "supports_list_result": False,
            "base_endpoint_length": len("/target.json?"),
            "enable_url_length_check": False,
        }
        config = EntityConfig(**config_dict)

        super().__init__(
            chembl_client=chembl_client,
            config=config,
            batch_size=batch_size,
            max_url_length=max_url_length,
        )
