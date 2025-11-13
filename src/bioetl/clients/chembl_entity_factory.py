"""Фабрика для создания клиентов ChEMBL-сущностей."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, MutableMapping

from bioetl.clients.chembl_entity_registry import ChemblEntityDefinition, get_entity_definition
from bioetl.clients.client_chembl_common import ChemblClient
from bioetl.config.models import PipelineConfig
from bioetl.config.models.source import SourceConfig
from bioetl.core.http.api_client import UnifiedAPIClient
from bioetl.core.http.client_factory import APIClientFactory
from bioetl.core.logging import LogEvents, UnifiedLogger

__all__ = ["ChemblClientBundle", "ChemblEntityClientFactory"]


@dataclass(frozen=True, slots=True)
class ChemblClientBundle:
    """Результат работы фабрики: сущностный клиент и сопутствующие объекты."""

    entity_name: str
    source_name: str
    base_url: str
    api_client: UnifiedAPIClient
    chembl_client: ChemblClient
    entity_client: Any | None
    entity_config: Any | None
    source_config: SourceConfig | None


class ChemblEntityClientFactory:
    """Единая точка создания клиентов ChEMBL-сущностей."""

    def __init__(
        self,
        pipeline_config: PipelineConfig,
        *,
        api_client_factory: APIClientFactory | None = None,
    ) -> None:
        self._config = pipeline_config
        self._api_factory = api_client_factory or APIClientFactory(pipeline_config)
        self._log = UnifiedLogger.get(__name__).bind(component="chembl_entity_factory")
        self._http_cache: MutableMapping[tuple[str, str], UnifiedAPIClient] = {}

    def build(
        self,
        entity_name: str,
        *,
        source_name: str = "chembl",
        source_config: SourceConfig | None = None,
        options: Mapping[str, Any] | None = None,
        chembl_client_kwargs: Mapping[str, Any] | None = None,
        fresh_http_client: bool = False,
    ) -> ChemblClientBundle:
        """Создать сущностный клиент и вернуть бандл с зависимостями."""

        definition = get_entity_definition(entity_name)
        resolved_source = source_config or self._resolve_source_config(source_name)
        base_url = self._resolve_base_url(resolved_source, options)
        http_client = self._get_http_client(
            source_name,
            base_url,
            fresh=fresh_http_client,
        )

        chembl_kwargs = dict(chembl_client_kwargs or {})
        chembl_client = ChemblClient(http_client, **chembl_kwargs)
        entity_client = self._build_entity_client(definition, chembl_client, resolved_source, options)

        self._log.debug(
            LogEvents.CLIENT_FACTORY_BUILD,
            entity=entity_name,
            source=source_name,
            base_url=base_url,
            cached_http=not fresh_http_client and (source_name, base_url) in self._http_cache,
            entity_client_type=type(entity_client).__name__ if entity_client is not None else None,
        )

        return ChemblClientBundle(
            entity_name=entity_name,
            source_name=source_name,
            base_url=base_url,
            api_client=http_client,
            chembl_client=chembl_client,
            entity_client=entity_client,
            entity_config=definition.entity_config,
            source_config=resolved_source,
        )

    # ------------------------------------------------------------------ #
    # Внутренние помощники                                              #
    # ------------------------------------------------------------------ #

    def build_http_client(
        self,
        *,
        source_name: str = "chembl",
        source_config: SourceConfig | None = None,
        options: Mapping[str, Any] | None = None,
        fresh_http_client: bool = False,
    ) -> tuple[UnifiedAPIClient, str, SourceConfig]:
        """Создать HTTP-клиент без сущностного уровня."""

        resolved_source = source_config or self._resolve_source_config(source_name)
        base_url = self._resolve_base_url(resolved_source, options)
        http_client = self._get_http_client(
            source_name,
            base_url,
            fresh=fresh_http_client,
        )
        return http_client, base_url, resolved_source

    def _get_http_client(
        self,
        source_name: str,
        base_url: str,
        *,
        fresh: bool,
    ) -> UnifiedAPIClient:
        cache_key = (source_name, base_url)
        if not fresh:
            cached = self._http_cache.get(cache_key)
            if cached is not None:
                return cached
        http_client = self._api_factory.for_source(source_name, base_url=base_url)
        self._http_cache[cache_key] = http_client
        return http_client

    def _resolve_source_config(self, source_name: str) -> SourceConfig:
        try:
            return self._config.sources[source_name]
        except KeyError as exc:
            msg = f"Источник '{source_name}' не найден в конфигурации пайплайна"
            raise KeyError(msg) from exc

    @staticmethod
    def _resolve_base_url(
        source_config: SourceConfig | None,
        options: Mapping[str, Any] | None,
    ) -> str:
        candidate = None
        if options is not None and "base_url" in options:
            candidate = options["base_url"]
        if candidate is None and source_config is not None:
            candidate = source_config.parameters.get("base_url")
        if candidate is None:
            candidate = "https://www.ebi.ac.uk/chembl/api/data"

        if not isinstance(candidate, str):
            msg = "base_url должен быть строкой"
            raise TypeError(msg)
        normalized = candidate.strip()
        if not normalized:
            msg = "base_url не может быть пустым"
            raise ValueError(msg)
        return normalized.rstrip("/")

    @staticmethod
    def _build_entity_client(
        definition: ChemblEntityDefinition,
        chembl_client: ChemblClient,
        source_config: SourceConfig | None,
        options: Mapping[str, Any] | None,
    ) -> Any:
        try:
            return definition.build_client(chembl_client, source_config, options)
        except Exception as exc:  # pragma: no cover - защитный блок
            msg = f"Не удалось создать клиент для сущности '{definition.name}'"
            raise RuntimeError(msg) from exc


