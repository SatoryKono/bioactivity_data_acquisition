"""Фабрика для создания клиентов ChEMBL-сущностей."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, MutableMapping, cast

from bioetl.clients.chembl_entity_registry import ChemblEntityDefinition, get_entity_definition
from bioetl.clients.client_chembl_common import ChemblClient
from bioetl.config.models import PipelineConfig
from bioetl.core.http.api_client import UnifiedAPIClient
from bioetl.core.http.client_factory import APIClientFactory
from bioetl.core.logging import LogEvents, UnifiedLogger

__all__ = ["ChemblClientBundle", "ChemblEntityClientFactory"]

@dataclass(frozen=True, slots=True)
class ChemblClientBundle:
    """Результат работы фабрики: сущностный клиент и сопутствующие объекты.

    Содержит все объекты, необходимые для работы с конкретной ChEMBL-сущностью:
    HTTP-клиент, ChEMBL-клиент, entity-клиент и их конфигурации.

    Parameters
    ----------
    entity_name : str
        Имя сущности, для которой создан клиент (например, "activity", "assay").
    source_name : str
        Имя источника данных из конфигурации пайплайна (обычно "chembl").
    base_url : str
        Базовый URL API ChEMBL, использованный при создании клиентов.
    api_client : UnifiedAPIClient
        HTTP-клиент для выполнения запросов к ChEMBL API.
    chembl_client : ChemblClient
        Высокоуровневый клиент ChEMBL с методами пагинации и handshake.
    entity_client : Any | None
        Специализированный клиент сущности (например, ChemblActivityClient).
        Может быть None, если сущность не требует специализированного клиента.
    entity_config : Any | None
        Конфигурация EntityConfig для данной сущности. Может быть None
        для сущностей без конфигурации (например, compound_record).
    source_config : Any | None
        Конфигурация источника SourceConfig из pipeline_config.sources.
        Может быть None, если источник не найден в конфигурации.

    Examples
    --------
    >>> bundle = factory.build("activity")
    >>> bundle.entity_client.fetch_by_ids(["CHEMBL123"])
    >>> bundle.chembl_client.handshake()
    """

    entity_name: str
    source_name: str
    base_url: str
    api_client: UnifiedAPIClient
    chembl_client: ChemblClient
    entity_client: Any | None
    entity_config: Any | None
    source_config: Any | None


class ChemblEntityClientFactory:
    """Единая точка создания клиентов ChEMBL-сущностей.

    Фабрика создаёт полный набор клиентов для работы с конкретной ChEMBL-сущностью:
    HTTP-клиент, ChEMBL-клиент и специализированный entity-клиент. Использует
    реестр сущностей (ChemblEntityDefinition) для определения типа клиента и
    конфигурации. Кэширует HTTP-клиенты для переиспользования.

    Parameters
    ----------
    pipeline_config : PipelineConfig
        Конфигурация пайплайна, содержащая настройки источников данных
        и HTTP-профили.
    api_client_factory : APIClientFactory | None, optional
        Фабрика для создания HTTP-клиентов. Если None, создаётся новая
        фабрика на основе pipeline_config. По умолчанию None.

    Attributes
    ----------
    _http_cache : MutableMapping[tuple[str, str], UnifiedAPIClient]
        Кэш HTTP-клиентов по ключу (source_name, base_url) для переиспользования.

    Examples
    --------
    >>> factory = ChemblEntityClientFactory(pipeline_config)
    >>> bundle = factory.build("activity", source_name="chembl")
    >>> df = bundle.entity_client.fetch_by_ids(["CHEMBL123"])
    """

    def __init__(
        self,
        pipeline_config: PipelineConfig,
        *,
        api_client_factory: APIClientFactory | None = None,
    ) -> None:
        """Инициализирует фабрику клиентов ChEMBL-сущностей.

        Parameters
        ----------
        pipeline_config : PipelineConfig
            Конфигурация пайплайна.
        api_client_factory : APIClientFactory | None, optional
            Фабрика для создания HTTP-клиентов. По умолчанию None.
        """
        self._config = pipeline_config
        self._api_factory = api_client_factory or APIClientFactory(pipeline_config)
        self._log = UnifiedLogger.get(__name__).bind(component="chembl_entity_factory")
        self._http_cache: MutableMapping[tuple[str, str], UnifiedAPIClient] = {}

    def build(
        self,
        entity_name: str,
        *,
        source_name: str = "chembl",
        source_config: Any | None = None,
        options: Mapping[str, Any] | None = None,
        chembl_client_kwargs: Mapping[str, Any] | None = None,
        fresh_http_client: bool = False,
    ) -> ChemblClientBundle:
        """Создаёт сущностный клиент и возвращает бандл с зависимостями.

        Процесс создания:
        1. Получает определение сущности из реестра по entity_name.
        2. Разрешает конфигурацию источника (source_config или из pipeline_config).
        3. Определяет base_url из options, source_config или значения по умолчанию.
        4. Создаёт или получает из кэша HTTP-клиент.
        5. Создаёт ChemblClient с опциональными параметрами.
        6. Создаёт специализированный entity-клиент через определение сущности.

        Parameters
        ----------
        entity_name : str
            Имя сущности из реестра (например, "activity", "assay", "target").
        source_name : str, optional
            Имя источника данных из pipeline_config.sources. Используется для
            получения конфигурации источника, если source_config не указан.
            По умолчанию "chembl".
        source_config : Any | None, optional
            Явная конфигурация источника SourceConfig. Если указана, используется
            вместо поиска в pipeline_config.sources. По умолчанию None.
        options : Mapping[str, Any] | None, optional
            Дополнительные опции для создания клиента (например, {"base_url": "..."}).
            base_url из options имеет приоритет над source_config. По умолчанию None.
        chembl_client_kwargs : Mapping[str, Any] | None, optional
            Дополнительные аргументы для конструктора ChemblClient
            (например, {"load_meta_store": store, "job_id": "123"}).
            По умолчанию None.
        fresh_http_client : bool, optional
            Если True, создаёт новый HTTP-клиент, игнорируя кэш. По умолчанию False.

        Returns
        -------
        ChemblClientBundle
            Бандл с созданными клиентами и конфигурациями.

        Raises
        ------
        ChemblEntityRegistryError
            Если сущность с указанным именем не зарегистрирована в реестре.
        KeyError
            Если source_name не найден в pipeline_config.sources и source_config не указан.
        TypeError
            Если base_url не является строкой.
        ValueError
            Если base_url пуст.
        RuntimeError
            Если не удалось создать entity-клиент (оборачивает исключения из build_client).

        Examples
        --------
        >>> factory = ChemblEntityClientFactory(pipeline_config)
        >>> bundle = factory.build("activity")
        >>> df = bundle.entity_client.fetch_by_ids(["CHEMBL123"])
        >>> bundle = factory.build(
        ...     "assay",
        ...     options={"base_url": "https://custom.chembl.api/data"},
        ...     fresh_http_client=True,
        ... )
        """
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
        source_config: Any | None = None,
        options: Mapping[str, Any] | None = None,
        fresh_http_client: bool = False,
    ) -> tuple[UnifiedAPIClient, str, Any]:
        """Создаёт HTTP-клиент без сущностного уровня.

        Удобный метод для получения только HTTP-клиента и связанных конфигураций
        без создания полного бандла с entity-клиентом.

        Parameters
        ----------
        source_name : str, optional
            Имя источника данных. По умолчанию "chembl".
        source_config : Any | None, optional
            Явная конфигурация источника. По умолчанию None.
        options : Mapping[str, Any] | None, optional
            Дополнительные опции (например, base_url). По умолчанию None.
        fresh_http_client : bool, optional
            Если True, создаёт новый HTTP-клиент, игнорируя кэш. По умолчанию False.

        Returns
        -------
        tuple[UnifiedAPIClient, str, Any]
            Кортеж из (HTTP-клиент, base_url, source_config).

        Raises
        ------
        KeyError
            Если source_name не найден в pipeline_config.sources и source_config не указан.
        TypeError
            Если base_url не является строкой.
        ValueError
            Если base_url пуст.
        """
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

    def _resolve_source_config(self, source_name: str) -> Any:
        try:
            return self._config.sources[source_name]
        except KeyError as exc:
            msg = f"Источник '{source_name}' не найден в конфигурации пайплайна"
            raise KeyError(msg) from exc

    @staticmethod
    def _resolve_base_url(
        source_config: Any | None,
        options: Mapping[str, Any] | None,
    ) -> str:
        candidate = None
        if options is not None and "base_url" in options:
            candidate = options["base_url"]
        if candidate is None and source_config is not None:
            parameters = ChemblEntityClientFactory._normalize_parameters(source_config)
            candidate = parameters.get("base_url")
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
    def _normalize_parameters(source_config: Any | None) -> Mapping[str, Any]:
        if source_config is None:
            return {}
        parameters = getattr(source_config, "parameters", {})
        if isinstance(parameters, Mapping):
            return cast(Mapping[str, Any], parameters)
        model_dump = getattr(parameters, "model_dump", None)
        if callable(model_dump):
            dumped = model_dump()
            if isinstance(dumped, Mapping):
                return cast(Mapping[str, Any], dumped)
        as_dict = getattr(parameters, "dict", None)
        if callable(as_dict):
            dumped = as_dict()
            if isinstance(dumped, Mapping):
                return cast(Mapping[str, Any], dumped)
        attrs = getattr(parameters, "__dict__", None)
        if isinstance(attrs, dict):
            return {str(key): value for key, value in attrs.items() if not key.startswith("_")}
        return {}

    @staticmethod
    def _build_entity_client(
        definition: ChemblEntityDefinition,
        chembl_client: ChemblClient,
        source_config: Any | None,
        options: Mapping[str, Any] | None,
    ) -> Any:
        try:
            return definition.build_client(chembl_client, source_config, options)
        except Exception as exc:  # pragma: no cover - защитный блок
            msg = f"Не удалось создать клиент для сущности '{definition.name}'"
            raise RuntimeError(msg) from exc


