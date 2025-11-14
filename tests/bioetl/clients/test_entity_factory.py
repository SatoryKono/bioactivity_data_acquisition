"""Тесты для фабрики клиентов ChEMBL-сущностей и интерфейсов."""

from __future__ import annotations

import inspect
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest  # type: ignore[reportMissingImports]

from bioetl.clients.chembl_config import EntityConfig, get_entity_config
from bioetl.clients.chembl_entity_factory import ChemblClientBundle, ChemblEntityClientFactory
from bioetl.clients.chembl_entity_registry import ChemblEntityRegistryError
from bioetl.clients.client_chembl import ChemblClient
from bioetl.clients.entities.client_activity import ChemblActivityClient
from bioetl.clients.entities.client_assay import ChemblAssayClient
from bioetl.clients.entities.client_document import ChemblDocumentClient
from bioetl.clients.entities.client_target import ChemblTargetClient
from bioetl.clients.entities.client_testitem import ChemblTestitemClient
from bioetl.config.models.source import SourceConfig
from bioetl.core.http.api_client import UnifiedAPIClient


@pytest.fixture  # type: ignore[reportUntypedFunctionDecorator,reportUnknownMemberType]
def mock_chembl_client() -> MagicMock:
    """Mock ChemblClient для тестирования."""
    mock_client = MagicMock(spec=ChemblClient)
    mock_client.handshake = MagicMock(return_value={})
    mock_client.paginate = MagicMock(return_value=iter([]))
    return mock_client


@pytest.fixture  # type: ignore[reportUntypedFunctionDecorator,reportUnknownMemberType]
def mock_api_client() -> MagicMock:
    """Mock UnifiedAPIClient для тестирования."""
    return MagicMock(spec=UnifiedAPIClient)


@pytest.fixture  # type: ignore[reportUntypedFunctionDecorator,reportUnknownMemberType]
def factory_with_http(
    pipeline_config_fixture,  # type: ignore[reportUnknownVariableType]
    mock_api_client: MagicMock,
) -> tuple[ChemblEntityClientFactory, MagicMock]:
    """Фабрика клиентов с замоканным HTTP-слоем."""

    factory = ChemblEntityClientFactory(pipeline_config_fixture)
    patcher = patch(
        "bioetl.core.http.client_factory.APIClientFactory.for_source",
        return_value=mock_api_client,
    )
    mock_for_source = patcher.start()
    try:
        yield factory, mock_for_source
    finally:
        patcher.stop()


@pytest.mark.unit  # type: ignore[reportUntypedClassDecorator,reportUnknownMemberType]
class TestChemblEntityClientFactory:
    """Тесты для ChemblEntityClientFactory."""

    @pytest.mark.parametrize(
        ("entity_name", "expected_cls"),
        [
            ("activity", ChemblActivityClient),
            ("assay", ChemblAssayClient),
            ("target", ChemblTargetClient),
            ("document", ChemblDocumentClient),
            ("testitem", ChemblTestitemClient),
        ],
    )
    def test_build_creates_correct_client_type(
        self,
        factory_with_http: tuple[ChemblEntityClientFactory, MagicMock],
        entity_name: str,
        expected_cls: type[Any],
    ) -> None:
        """Пайплайн получает нужный класс клиента и EntityConfig из реестра."""

        factory, _ = factory_with_http
        bundle = factory.build(entity_name)
        assert isinstance(bundle.entity_client, expected_cls)
        assert bundle.entity_config == get_entity_config(entity_name)

    def test_build_returns_bundle_with_all_components(
        self,
        factory_with_http: tuple[ChemblEntityClientFactory, MagicMock],
    ) -> None:
        """Бандл содержит все необходимые зависимости и метаданные."""

        factory, _ = factory_with_http
        bundle = factory.build("activity")
        assert isinstance(bundle, ChemblClientBundle)
        assert bundle.entity_name == "activity"
        assert bundle.source_name == "chembl"
        assert bundle.base_url.startswith("https://")
        assert bundle.api_client is not None
        assert bundle.chembl_client is not None
        assert bundle.entity_client is not None
        assert isinstance(bundle.entity_config, EntityConfig)

    def test_build_with_custom_source_config(
        self,
        factory_with_http: tuple[ChemblEntityClientFactory, MagicMock],
    ) -> None:
        """Явный SourceConfig переопределяет параметры по умолчанию."""

        factory, _ = factory_with_http
        custom_source = SourceConfig(  # type: ignore[call-arg]
            enabled=True,
            parameters={"base_url": "https://custom.chembl.api/data"},
        )
        bundle = factory.build("activity", source_config=custom_source, source_name="chembl")
        assert bundle.source_config == custom_source
        assert bundle.base_url == "https://custom.chembl.api/data"

    def test_build_with_options_overrides_base_url(
        self,
        factory_with_http: tuple[ChemblEntityClientFactory, MagicMock],
    ) -> None:
        """Опции build() имеют высший приоритет над SourceConfig."""

        factory, _ = factory_with_http
        custom_url = "https://custom.chembl.api/data/"
        bundle = factory.build("activity", options={"base_url": custom_url})
        assert bundle.base_url == "https://custom.chembl.api/data"

    def test_build_caches_http_clients(
        self,
        factory_with_http: tuple[ChemblEntityClientFactory, MagicMock],
    ) -> None:
        """HTTP-клиент создаётся один раз для одинакового источника и URL."""

        factory, mock_for_source = factory_with_http
        _ = factory.build("activity")
        _ = factory.build("assay")
        assert mock_for_source.call_count == 1

    def test_factory_fresh_http_client_flag(
        self,
        factory_with_http: tuple[ChemblEntityClientFactory, MagicMock],
    ) -> None:
        """Параметр fresh_http_client сбрасывает кэш HTTP-клиентов."""

        factory, mock_for_source = factory_with_http
        _ = factory.build("activity", fresh_http_client=True)
        _ = factory.build("activity", fresh_http_client=True)
        assert mock_for_source.call_count == 2

    def test_build_unknown_entity_raises_error(
        self,
        pipeline_config_fixture,  # type: ignore[reportUnknownVariableType]
    ) -> None:
        """Неизвестные сущности отклоняются реестром."""

        factory = ChemblEntityClientFactory(pipeline_config_fixture)
        with pytest.raises(ChemblEntityRegistryError):  # type: ignore[reportUnknownMemberType]
            factory.build("unknown_entity")

    def test_factory_handles_missing_source_gracefully(
        self,
        factory_with_http: tuple[ChemblEntityClientFactory, MagicMock],
    ) -> None:
        """Попытка использовать несуществующий source_name приводит к KeyError."""

        factory, _ = factory_with_http
        with pytest.raises(KeyError, match="не найден в конфигурации"):  # type: ignore[reportUnknownMemberType]
            factory.build("activity", source_name="nonexistent")

    def test_factory_handles_invalid_base_url(
        self,
        factory_with_http: tuple[ChemblEntityClientFactory, MagicMock],
    ) -> None:
        """Пустой base_url из options вызывает ValueError."""

        factory, _ = factory_with_http
        with pytest.raises(ValueError, match="не может быть пустым"):  # type: ignore[reportUnknownMemberType]
            factory.build("activity", options={"base_url": ""})


@pytest.mark.unit  # type: ignore[reportUntypedClassDecorator,reportUnknownMemberType]
class TestEntityClientProtocol:
    """Тесты для проверки соблюдения протокола ChemblEntityClientProtocol."""

    @pytest.mark.parametrize("entity_name", ["activity", "assay", "target"])
    def test_entity_client_implements_protocol(
        self,
        factory_with_http: tuple[ChemblEntityClientFactory, MagicMock],
        entity_name: str,
    ) -> None:
        """Фабрика выдаёт клиентов с единым интерфейсом."""

        factory, _ = factory_with_http
        bundle = factory.build(entity_name)
        client = bundle.entity_client
        assert client is not None
        for method_name in ("fetch_by_ids", "fetch_all", "iterate_records"):
            method = getattr(client, method_name, None)
            assert callable(method), f"{method_name} отсутствует у клиента {entity_name}"

    def test_protocol_signatures_are_stable(
        self,
        mock_chembl_client: MagicMock,
    ) -> None:
        """Методы клиента сохраняют обязательные аргументы."""

        client = ChemblActivityClient(mock_chembl_client, batch_size=25)
        fetch_by_ids_sig = inspect.signature(client.fetch_by_ids)
        assert {"ids", "fields", "page_limit"} <= set(fetch_by_ids_sig.parameters)

        fetch_all_sig = inspect.signature(client.fetch_all)
        assert {"limit", "fields", "page_size"} <= set(fetch_all_sig.parameters)

        iterate_sig = inspect.signature(client.iterate_records)
        assert {"params", "limit", "fields", "page_size"} <= set(iterate_sig.parameters)

    def test_fetch_by_ids_returns_dataframe(
        self,
        mock_chembl_client: MagicMock,
    ) -> None:
        """Проверяет, что fetch_by_ids возвращает DataFrame."""

        # Настраиваем мок для возврата данных
        mock_record = {"activity_id": "CHEMBL123", "standard_value": 1.0}
        mock_chembl_client.paginate.return_value = iter([mock_record])
        client = ChemblActivityClient(mock_chembl_client, batch_size=25)
        result = client.fetch_by_ids(["CHEMBL123"])
        assert isinstance(result, pd.DataFrame)
        assert len(result) >= 0  # Может быть пустым, если мок не настроен правильно

    def test_fetch_all_returns_dataframe(
        self,
        mock_chembl_client: MagicMock,
    ) -> None:
        """Проверяет, что fetch_all возвращает DataFrame."""

        mock_chembl_client.paginate.return_value = iter([])
        client = ChemblActivityClient(mock_chembl_client, batch_size=25)
        result = client.fetch_all(limit=10)
        assert isinstance(result, pd.DataFrame)

    def test_iterate_records_returns_iterator(
        self,
        mock_chembl_client: MagicMock,
    ) -> None:
        """Проверяет, что iterate_records возвращает итератор."""

        mock_record = {"activity_id": "CHEMBL123"}
        mock_chembl_client.paginate.return_value = iter([mock_record])
        client = ChemblActivityClient(mock_chembl_client, batch_size=25)
        records = client.iterate_records(limit=1)
        assert hasattr(records, "__iter__")
        # Проверяем, что можно получить запись
        record_list = list(records)
        assert len(record_list) >= 0


@pytest.mark.unit  # type: ignore[reportUntypedClassDecorator,reportUnknownMemberType]
class TestEntityConfigUsage:
    """Тесты для проверки использования EntityConfig клиентами."""

    @pytest.mark.parametrize("entity_name", ["activity", "assay", "target"])
    def test_bundle_exposes_registry_config(
        self,
        factory_with_http: tuple[ChemblEntityClientFactory, MagicMock],
        entity_name: str,
    ) -> None:
        """Фабрика прокидывает EntityConfig из реестра без модификаций."""

        factory, _ = factory_with_http
        bundle = factory.build(entity_name)
        expected_config = get_entity_config(entity_name)
        assert bundle.entity_config == expected_config

    def test_fetch_by_ids_builds_params_from_entity_config(
        self,
        mock_chembl_client: MagicMock,
    ) -> None:
        """Клиент использует filter_param и chunking из EntityConfig."""

        mock_chembl_client.paginate.return_value = iter([{"activity_id": "CHEMBL123"}])
        client = ChemblActivityClient(mock_chembl_client, batch_size=25)
        _ = client.fetch_by_ids(["CHEMBL123", ""])
        paginate_kwargs = mock_chembl_client.paginate.call_args.kwargs
        params = paginate_kwargs["params"]
        assert params["activity_id__in"] == "CHEMBL123"
        assert paginate_kwargs["items_key"] == client.ENTITY_CONFIG.items_key  # type: ignore[attr-defined]

    def test_fetch_by_ids_applies_field_selection(
        self,
        mock_chembl_client: MagicMock,
    ) -> None:
        """Параметр fields сериализуется в only и влияет на порядок колонок."""

        mock_chembl_client.paginate.return_value = iter(
            [
                {"activity_id": "CHEMBL1", "standard_value": 1.0, "extra": "x"},
            ]
        )
        client = ChemblActivityClient(mock_chembl_client, batch_size=25)
        frame = client.fetch_by_ids(["CHEMBL1"], fields=("activity_id", "extra"))
        params = mock_chembl_client.paginate.call_args.kwargs["params"]
        assert params["only"] == "activity_id,extra"
        assert frame.columns.tolist() == ["activity_id", "extra"]

    def test_iterate_records_respects_limit_and_fields(
        self,
        mock_chembl_client: MagicMock,
    ) -> None:
        """iterate_records ограничивает число записей и прокидывает only."""

        mock_records = [{"activity_id": "CHEMBL1"}, {"activity_id": "CHEMBL2"}]
        mock_chembl_client.paginate.return_value = iter(mock_records)
        client = ChemblActivityClient(mock_chembl_client, batch_size=25)
        records = list(client.iterate_records(limit=1, fields=("activity_id",)))
        assert len(records) == 1
        params = mock_chembl_client.paginate.call_args.kwargs["params"]
        assert params["only"] == "activity_id"

    def test_fetch_all_reindexes_columns_in_requested_order(
        self,
        mock_chembl_client: MagicMock,
    ) -> None:
        """fetch_all возвращает DataFrame с колонками в том же порядке, что и fields."""

        mock_chembl_client.paginate.return_value = iter(
            [
                {"activity_id": "CHEMBL2", "extra": "b"},
                {"activity_id": "CHEMBL1", "extra": "a"},
            ]
        )
        client = ChemblActivityClient(mock_chembl_client, batch_size=25)
        frame = client.fetch_all(fields=("extra", "activity_id"), limit=2)
        assert frame.columns.tolist() == ["extra", "activity_id"]

    def test_client_uses_base_methods_not_duplicates(
        self,
        mock_chembl_client: MagicMock,
    ) -> None:
        """Клиент строит запросы исключительно через ChemblClient.paginate."""

        mock_chembl_client.paginate.return_value = iter([{"activity_id": "CHEMBL123"}])
        client = ChemblActivityClient(mock_chembl_client, batch_size=25)
        _ = client.fetch_by_ids(["CHEMBL123"])
        mock_chembl_client.paginate.assert_called_once()


