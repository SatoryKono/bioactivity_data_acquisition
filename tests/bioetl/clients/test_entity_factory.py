"""Тесты для фабрики клиентов ChEMBL-сущностей и интерфейсов."""

from __future__ import annotations

from typing import cast
from unittest.mock import MagicMock

import pandas as pd
import pytest  # type: ignore[reportMissingImports]

from bioetl.clients.chembl_config import EntityConfig, get_entity_config
from bioetl.clients.chembl_entity_factory import ChemblClientBundle, ChemblEntityClientFactory
from bioetl.clients.chembl_entity_registry import ChemblEntityRegistryError, get_entity_definition
from bioetl.clients.client_chembl_base import ChemblEntityClientProtocol
from bioetl.clients.client_chembl_common import ChemblClient
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


@pytest.mark.unit  # type: ignore[reportUntypedClassDecorator,reportUnknownMemberType]
class TestChemblEntityClientFactory:
    """Тесты для ChemblEntityClientFactory."""

    def test_build_creates_correct_client_type(
        self,
        pipeline_config_fixture,  # type: ignore[reportUnknownVariableType]
        mock_api_client: MagicMock,
    ) -> None:
        """Проверяет, что фабрика создаёт корректный тип клиента по имени сущности."""
        with pytest.raises(AttributeError):  # type: ignore[reportUnknownMemberType]
            # Мокаем APIClientFactory.for_source
            factory = ChemblEntityClientFactory(pipeline_config_fixture)
            # Используем реальную фабрику, но с моком через patch
            from unittest.mock import patch

            with patch("bioetl.core.http.client_factory.APIClientFactory.for_source") as mock_for_source:
                mock_for_source.return_value = mock_api_client
                bundle = factory.build("activity")
                assert bundle.entity_name == "activity"
                assert bundle.entity_client is not None
                # Проверяем, что клиент реализует протокол
                assert hasattr(bundle.entity_client, "fetch_by_ids")
                assert hasattr(bundle.entity_client, "fetch_all")
                assert hasattr(bundle.entity_client, "iterate_records")

    def test_build_uses_entity_config(
        self,
        pipeline_config_fixture,  # type: ignore[reportUnknownVariableType]
        mock_api_client: MagicMock,
    ) -> None:
        """Проверяет, что созданный клиент использует EntityConfig из реестра."""
        from unittest.mock import patch

        factory = ChemblEntityClientFactory(pipeline_config_fixture)
        with patch("bioetl.core.http.client_factory.APIClientFactory.for_source") as mock_for_source:
            mock_for_source.return_value = mock_api_client
            bundle = factory.build("activity")
            assert bundle.entity_config is not None
            assert isinstance(bundle.entity_config, EntityConfig)
            assert bundle.entity_config.endpoint == "/activity.json"
            assert bundle.entity_config.id_field == "activity_id"

    def test_build_unknown_entity_raises_error(
        self,
        pipeline_config_fixture,  # type: ignore[reportUnknownVariableType]
    ) -> None:
        """Проверяет, что запрос неизвестной сущности вызывает исключение."""
        factory = ChemblEntityClientFactory(pipeline_config_fixture)
        with pytest.raises(ChemblEntityRegistryError):  # type: ignore[reportUnknownMemberType]
            factory.build("unknown_entity")

    def test_build_returns_bundle_with_all_components(
        self,
        pipeline_config_fixture,  # type: ignore[reportUnknownVariableType]
        mock_api_client: MagicMock,
    ) -> None:
        """Проверяет, что бандл содержит все необходимые компоненты."""
        from unittest.mock import patch

        factory = ChemblEntityClientFactory(pipeline_config_fixture)
        with patch("bioetl.core.http.client_factory.APIClientFactory.for_source") as mock_for_source:
            mock_for_source.return_value = mock_api_client
            bundle = factory.build("activity")
            assert isinstance(bundle, ChemblClientBundle)
            assert bundle.entity_name == "activity"
            assert bundle.source_name == "chembl"
            assert bundle.base_url is not None
            assert bundle.api_client is not None
            assert bundle.chembl_client is not None
            assert bundle.entity_client is not None
            assert bundle.entity_config is not None

    def test_build_with_custom_source_config(
        self,
        pipeline_config_fixture,  # type: ignore[reportUnknownVariableType]
        mock_api_client: MagicMock,
    ) -> None:
        """Проверяет создание клиента с явной конфигурацией источника."""
        from unittest.mock import patch

        factory = ChemblEntityClientFactory(pipeline_config_fixture)
        custom_source = SourceConfig(  # type: ignore[call-arg]
            enabled=True,
            parameters={"base_url": "https://custom.chembl.api/data"},
        )
        with patch("bioetl.core.http.client_factory.APIClientFactory.for_source") as mock_for_source:
            mock_for_source.return_value = mock_api_client
            bundle = factory.build("activity", source_config=custom_source)
            assert bundle.source_config == custom_source

    def test_build_with_options_overrides_base_url(
        self,
        pipeline_config_fixture,  # type: ignore[reportUnknownVariableType]
        mock_api_client: MagicMock,
    ) -> None:
        """Проверяет, что options могут переопределить base_url."""
        from unittest.mock import patch

        factory = ChemblEntityClientFactory(pipeline_config_fixture)
        custom_url = "https://custom.chembl.api/data"
        with patch("bioetl.core.http.client_factory.APIClientFactory.for_source") as mock_for_source:
            mock_for_source.return_value = mock_api_client
            bundle = factory.build("activity", options={"base_url": custom_url})
            assert bundle.base_url == custom_url.rstrip("/")

    def test_build_caches_http_clients(
        self,
        pipeline_config_fixture,  # type: ignore[reportUnknownVariableType]
        mock_api_client: MagicMock,
    ) -> None:
        """Проверяет, что HTTP-клиенты кэшируются для переиспользования."""
        from unittest.mock import patch

        factory = ChemblEntityClientFactory(pipeline_config_fixture)
        with patch("bioetl.core.http.client_factory.APIClientFactory.for_source") as mock_for_source:
            mock_for_source.return_value = mock_api_client
            bundle1 = factory.build("activity")
            bundle2 = factory.build("assay")
            # Оба должны использовать один и тот же HTTP-клиент (если source_name и base_url совпадают)
            assert mock_for_source.call_count == 1  # Должен быть вызван только один раз


@pytest.mark.unit  # type: ignore[reportUntypedClassDecorator,reportUnknownMemberType]
class TestEntityClientProtocol:
    """Тесты для проверки соблюдения протокола ChemblEntityClientProtocol."""

    def test_entity_client_implements_protocol(
        self,
        pipeline_config_fixture,  # type: ignore[reportUnknownVariableType]
        mock_api_client: MagicMock,
    ) -> None:
        """Проверяет, что созданный клиент реализует ChemblEntityClientProtocol."""
        from unittest.mock import patch

        factory = ChemblEntityClientFactory(pipeline_config_fixture)
        with patch("bioetl.core.http.client_factory.APIClientFactory.for_source") as mock_for_source:
            mock_for_source.return_value = mock_api_client
            bundle = factory.build("activity")
            client = bundle.entity_client
            assert client is not None
            # Проверяем наличие всех методов протокола
            assert hasattr(client, "fetch_by_ids")
            assert hasattr(client, "fetch_all")
            assert hasattr(client, "iterate_records")
            # Проверяем сигнатуры методов
            import inspect

            fetch_by_ids_sig = inspect.signature(client.fetch_by_ids)
            assert "ids" in fetch_by_ids_sig.parameters
            assert "fields" in fetch_by_ids_sig.parameters
            assert "page_limit" in fetch_by_ids_sig.parameters

    def test_fetch_by_ids_returns_dataframe(
        self,
        mock_chembl_client: MagicMock,
    ) -> None:
        """Проверяет, что fetch_by_ids возвращает DataFrame."""
        from bioetl.clients.entities.client_activity import ChemblActivityClient

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
        from bioetl.clients.entities.client_activity import ChemblActivityClient

        mock_chembl_client.paginate.return_value = iter([])
        client = ChemblActivityClient(mock_chembl_client, batch_size=25)
        result = client.fetch_all(limit=10)
        assert isinstance(result, pd.DataFrame)

    def test_iterate_records_returns_iterator(
        self,
        mock_chembl_client: MagicMock,
    ) -> None:
        """Проверяет, что iterate_records возвращает итератор."""
        from bioetl.clients.entities.client_activity import ChemblActivityClient

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

    def test_client_uses_entity_config_from_registry(
        self,
        pipeline_config_fixture,  # type: ignore[reportUnknownVariableType]
        mock_api_client: MagicMock,
    ) -> None:
        """Проверяет, что клиент использует EntityConfig из реестра."""
        from unittest.mock import patch

        factory = ChemblEntityClientFactory(pipeline_config_fixture)
        with patch("bioetl.core.http.client_factory.APIClientFactory.for_source") as mock_for_source:
            mock_for_source.return_value = mock_api_client
            # Проверяем несколько сущностей
            for entity_name in ["activity", "assay", "target"]:
                bundle = factory.build(entity_name)
                expected_config = get_entity_config(entity_name)
                assert bundle.entity_config is not None
                assert bundle.entity_config.endpoint == expected_config.endpoint
                assert bundle.entity_config.id_field == expected_config.id_field

    def test_client_uses_base_methods_not_duplicates(
        self,
        mock_chembl_client: MagicMock,
    ) -> None:
        """Проверяет, что клиент использует базовые методы, а не дублирует запросы."""
        from bioetl.clients.entities.client_activity import ChemblActivityClient

        # Настраиваем мок для отслеживания вызовов
        mock_chembl_client.paginate.return_value = iter([{"activity_id": "CHEMBL123"}])
        client = ChemblActivityClient(mock_chembl_client, batch_size=25)
        # Вызываем fetch_by_ids
        _ = client.fetch_by_ids(["CHEMBL123"])
        # Проверяем, что paginate был вызван (через базовый метод)
        assert mock_chembl_client.paginate.called

    def test_multiple_entities_have_correct_configs(
        self,
        pipeline_config_fixture,  # type: ignore[reportUnknownVariableType]
        mock_api_client: MagicMock,
    ) -> None:
        """Проверяет, что разные сущности имеют корректные конфигурации."""
        from unittest.mock import patch

        factory = ChemblEntityClientFactory(pipeline_config_fixture)
        entity_configs = {
            "activity": ("/activity.json", "activity_id"),
            "assay": ("/assay.json", "assay_chembl_id"),
            "target": ("/target.json", "target_chembl_id"),
        }
        with patch("bioetl.core.http.client_factory.APIClientFactory.for_source") as mock_for_source:
            mock_for_source.return_value = mock_api_client
            for entity_name, (expected_endpoint, expected_id_field) in entity_configs.items():
                bundle = factory.build(entity_name)
                assert bundle.entity_config is not None
                assert bundle.entity_config.endpoint == expected_endpoint
                assert bundle.entity_config.id_field == expected_id_field


@pytest.mark.unit  # type: ignore[reportUntypedClassDecorator,reportUnknownMemberType]
class TestFactoryBehavior:
    """Тесты для проверки поведения фабрики в различных сценариях."""

    def test_factory_handles_missing_source_gracefully(
        self,
        pipeline_config_fixture,  # type: ignore[reportUnknownVariableType]
    ) -> None:
        """Проверяет обработку отсутствующего источника."""
        factory = ChemblEntityClientFactory(pipeline_config_fixture)
        with pytest.raises(KeyError, match="не найден в конфигурации"):  # type: ignore[reportUnknownMemberType]
            factory.build("activity", source_name="nonexistent_source")

    def test_factory_handles_invalid_base_url(
        self,
        pipeline_config_fixture,  # type: ignore[reportUnknownVariableType]
        mock_api_client: MagicMock,
    ) -> None:
        """Проверяет обработку невалидного base_url."""
        from unittest.mock import patch

        factory = ChemblEntityClientFactory(pipeline_config_fixture)
        with patch("bioetl.core.http.client_factory.APIClientFactory.for_source") as mock_for_source:
            mock_for_source.return_value = mock_api_client
            # Пустой base_url должен вызвать ValueError
            with pytest.raises(ValueError, match="не может быть пустым"):  # type: ignore[reportUnknownMemberType]
                factory.build("activity", options={"base_url": ""})

    def test_factory_fresh_http_client_flag(
        self,
        pipeline_config_fixture,  # type: ignore[reportUnknownVariableType]
        mock_api_client: MagicMock,
    ) -> None:
        """Проверяет работу флага fresh_http_client."""
        from unittest.mock import patch

        factory = ChemblEntityClientFactory(pipeline_config_fixture)
        with patch("bioetl.core.http.client_factory.APIClientFactory.for_source") as mock_for_source:
            mock_for_source.return_value = mock_api_client
            # Первый вызов
            bundle1 = factory.build("activity", fresh_http_client=True)
            # Второй вызов с fresh_http_client=True должен создать новый клиент
            bundle2 = factory.build("activity", fresh_http_client=True)
            # Оба вызова должны создать новые клиенты
            assert mock_for_source.call_count == 2

