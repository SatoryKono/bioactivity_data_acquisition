"""Chembl assay parameters entity client."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any, ClassVar

from bioetl.clients.client_chembl_base import EntityConfig, make_entity_config
from bioetl.clients.client_chembl_entity import ChemblEntityClientBase

__all__ = ["ChemblAssayParametersEntityClient"]


class ChemblAssayParametersEntityClient(ChemblEntityClientBase):
    """Клиент для получения assay_parameters записей из ChEMBL API."""

    CONFIG: ClassVar[EntityConfig] = make_entity_config(
        endpoint="/assay_parameter.json",
        filter_param="assay_chembl_id__in",
        id_key="assay_chembl_id",
        items_key="assay_parameters",
        log_prefix="assay_parameters",
        chunk_size=100,
        supports_list_result=True,  # Один assay может иметь несколько parameters
    )

    def fetch_by_ids(
        self,
        ids: Iterable[str],
        fields: Sequence[str],
        page_limit: int = 1000,
        active_only: bool = True,
    ) -> dict[str, list[dict[str, Any]]]:
        """Получить assay_parameters записи по ID.

        Parameters
        ----------
        ids:
            Итерируемый объект с ID для получения.
        fields:
            Список полей для получения из API.
        page_limit:
            Размер страницы для пагинации.
        active_only:
            Если True, фильтровать только активные параметры (active=1).

        Returns
        -------
        dict[str, list[dict[str, Any]]]:
            Словарь ID -> список записей.
        """
        # Переопределяем для добавления параметра active_only
        import pandas as pd

        unique_ids: set[str] = set()
        for entity_id in ids:
            if entity_id and not (isinstance(entity_id, float) and pd.isna(entity_id)):
                unique_ids.add(str(entity_id).strip())

        if not unique_ids:
            self._log.debug(
                f"{self._config.log_prefix}.no_ids",
                message="No valid IDs to fetch",
            )
            return {}

        all_records: list[dict[str, Any]] = []
        ids_list = list(unique_ids)

        for i in range(0, len(ids_list), self._config.chunk_size):
            chunk = ids_list[i : i + self._config.chunk_size]
            params: dict[str, Any] = {
                self._config.filter_param: ",".join(chunk),
                "limit": page_limit,
            }
            # Фильтр активных параметров, если запрошено
            if active_only:
                params["active"] = "1"
            # Параметр only для выбора полей
            if fields:
                params["only"] = ",".join(fields)

            try:
                for record in self._chembl_client.paginate(
                    self._config.endpoint,
                    params=params,
                    page_size=page_limit,
                    items_key=self._config.items_key,
                ):
                    all_records.append(dict(record))
            except Exception as exc:
                self._log.warning(
                    f"{self._config.log_prefix}.fetch_error",
                    entity_count=len(chunk),
                    error=str(exc),
                    exc_info=True,
                )

        return self._build_list_result(all_records, unique_ids)

