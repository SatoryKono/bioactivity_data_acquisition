"""Shared base implementation for high level ChEMBL entity clients."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import ClassVar

from bioetl.clients.chembl_base import ChemblClientProtocol, EntityConfig
from bioetl.clients.chembl_iterator import ChemblEntityIterator

__all__ = ["ChemblEntityClientBase"]


@dataclass(frozen=True)
class _EntityConfigTemplate:
    """Descriptor holding immutable configuration parameters for an entity."""

    endpoint: str
    filter_param: str
    id_key: str
    items_key: str
    log_prefix: str
    chunk_size: int = 100
    supports_list_result: bool = False
    base_endpoint_length: int | None = None
    enable_url_length_when_max: bool = False

    def build(self, max_url_length: int | None) -> EntityConfig:
        """Instantiate :class:`EntityConfig` honouring the URL length policy."""

        base_length = (
            self.base_endpoint_length
            if self.base_endpoint_length is not None
            else len(f"{self.endpoint}?")
        )

        enable_url_length_check = self.enable_url_length_when_max and max_url_length is not None

        return EntityConfig(
            endpoint=self.endpoint,
            filter_param=self.filter_param,
            id_key=self.id_key,
            items_key=self.items_key,
            log_prefix=self.log_prefix,
            chunk_size=self.chunk_size,
            supports_list_result=self.supports_list_result,
            base_endpoint_length=base_length,
            enable_url_length_check=enable_url_length_check,
        )


_ENTITY_CONFIG_REGISTRY: Mapping[str, _EntityConfigTemplate] = {
    "activity": _EntityConfigTemplate(
        endpoint="/activity.json",
        filter_param="activity_id__in",
        id_key="activity_id",
        items_key="activities",
        log_prefix="activity",
        chunk_size=100,
        supports_list_result=False,
        base_endpoint_length=len("/activity.json?"),
    ),
    "assay": _EntityConfigTemplate(
        endpoint="/assay.json",
        filter_param="assay_chembl_id__in",
        id_key="assay_chembl_id",
        items_key="assays",
        log_prefix="assay",
        chunk_size=100,
        supports_list_result=False,
        base_endpoint_length=len("/assay.json?"),
        enable_url_length_when_max=True,
    ),
    "document": _EntityConfigTemplate(
        endpoint="/document.json",
        filter_param="document_chembl_id__in",
        id_key="document_chembl_id",
        items_key="documents",
        log_prefix="document",
        chunk_size=100,
        supports_list_result=False,
        base_endpoint_length=len("/document.json?"),
    ),
    "target": _EntityConfigTemplate(
        endpoint="/target.json",
        filter_param="target_chembl_id__in",
        id_key="target_chembl_id",
        items_key="targets",
        log_prefix="target",
        chunk_size=100,
        supports_list_result=False,
        base_endpoint_length=len("/target.json?"),
    ),
    "testitem": _EntityConfigTemplate(
        endpoint="/molecule.json",
        filter_param="molecule_chembl_id__in",
        id_key="molecule_chembl_id",
        items_key="molecules",
        log_prefix="molecule",
        chunk_size=100,
        supports_list_result=False,
        base_endpoint_length=len("/molecule.json?"),
    ),
}


class ChemblEntityClientBase(ChemblEntityIterator):
    """Base class encapsulating common options for entity clients."""

    DEFAULT_BATCH_SIZE: ClassVar[int] = 25
    DEFAULT_MAX_URL_LENGTH: ClassVar[int | None] = None
    ENTITY_KEY: ClassVar[str]

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
    def _create_config(cls, max_url_length: int | None) -> EntityConfig:
        """Construct the entity configuration using the shared registry."""

        try:
            template = _ENTITY_CONFIG_REGISTRY[cls.ENTITY_KEY]
        except KeyError as exc:
            msg = f"Неизвестная сущность ChEMBL для {cls.__name__}: {cls.ENTITY_KEY!r}"
            raise KeyError(msg) from exc
        return template.build(max_url_length)

