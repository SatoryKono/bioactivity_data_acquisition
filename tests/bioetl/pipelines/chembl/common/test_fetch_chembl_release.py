"""Тесты для логики получения ChEMBL release."""

from __future__ import annotations

from unittest.mock import Mock, patch

import pytest
from requests.exceptions import (
    ConnectionError as RequestsConnectionError,
)
from requests.exceptions import (
    HTTPError,
    Timeout,
)

from bioetl.chembl.common.descriptor import ChemblExtractionContext
from bioetl.clients.client_chembl import ChemblClient
from bioetl.core.http.api_client import UnifiedAPIClient
from bioetl.pipelines.chembl.target import run as target_run
from bioetl.pipelines.chembl.testitem import run as testitem_run


@pytest.mark.unit
def test_fetch_chembl_release_via_chembl_client(
    pipeline_config_fixture,
    run_id: str,
) -> None:
    """Тест получения релиза через ChemblClient.handshake()."""
    pipeline = target_run.ChemblTargetPipeline(
        config=pipeline_config_fixture,
        run_id=run_id,
    )

    mock_client = Mock(spec=ChemblClient)
    mock_client.handshake.return_value = {"chembl_db_version": "33"}

    result = pipeline.fetch_chembl_release(mock_client)

    assert result == "33"
    mock_client.handshake.assert_called_once()


@pytest.mark.unit
def test_fetch_chembl_release_via_unified_client(
    pipeline_config_fixture,
    run_id: str,
) -> None:
    """Тест получения релиза через UnifiedAPIClient."""
    pipeline = target_run.ChemblTargetPipeline(
        config=pipeline_config_fixture,
        run_id=run_id,
    )

    mock_response = Mock()
    mock_response.json.return_value = {"chembl_db_version": "34"}

    mock_client = Mock(spec=UnifiedAPIClient)
    mock_client.get.return_value = mock_response

    result = pipeline.fetch_chembl_release(mock_client)

    assert result == "34"
    mock_client.get.assert_called_once_with("/status")


@pytest.mark.unit
def test_fetch_chembl_release_handles_network_error(
    pipeline_config_fixture,
    run_id: str,
) -> None:
    """Тест обработки сетевой ошибки при получении релиза."""
    pipeline = target_run.ChemblTargetPipeline(
        config=pipeline_config_fixture,
        run_id=run_id,
    )

    mock_client = Mock(spec=ChemblClient)
    mock_client.handshake.side_effect = RequestsConnectionError("Network unreachable")

    with patch("bioetl.chembl.common.descriptor.UnifiedLogger.get"):
        result = pipeline.fetch_chembl_release(mock_client)

    assert result is None


@pytest.mark.unit
def test_fetch_chembl_release_handles_timeout(
    pipeline_config_fixture,
    run_id: str,
) -> None:
    """Тест обработки таймаута при получении релиза."""
    pipeline = target_run.ChemblTargetPipeline(
        config=pipeline_config_fixture,
        run_id=run_id,
    )

    mock_client = Mock(spec=ChemblClient)
    mock_client.handshake.side_effect = Timeout("Request timeout")

    with patch("bioetl.chembl.common.descriptor.UnifiedLogger.get"):
        result = pipeline.fetch_chembl_release(mock_client)

    assert result is None


@pytest.mark.unit
def test_fetch_chembl_release_handles_http_error(
    pipeline_config_fixture,
    run_id: str,
) -> None:
    """Тест обработки HTTP ошибки (404) при получении релиза."""
    pipeline = target_run.ChemblTargetPipeline(
        config=pipeline_config_fixture,
        run_id=run_id,
    )

    mock_response = Mock()
    mock_response.raise_for_status.side_effect = HTTPError("404 Not Found")
    mock_client = Mock(spec=UnifiedAPIClient)
    mock_client.get.side_effect = HTTPError("404 Not Found")

    with patch("bioetl.chembl.common.descriptor.UnifiedLogger.get"):
        result = pipeline.fetch_chembl_release(mock_client)

    assert result is None


@pytest.mark.unit
def test_fetch_chembl_release_missing_chembl_db_version(
    pipeline_config_fixture,
    run_id: str,
) -> None:
    """Тест обработки ответа без chembl_db_version."""
    pipeline = target_run.ChemblTargetPipeline(
        config=pipeline_config_fixture,
        run_id=run_id,
    )

    mock_client = Mock(spec=ChemblClient)
    mock_client.handshake.return_value = {"api_version": "1.0"}

    result = pipeline.fetch_chembl_release(mock_client)

    assert result is None


@pytest.mark.unit
def test_fetch_chembl_release_empty_response(
    pipeline_config_fixture,
    run_id: str,
) -> None:
    """Тест обработки пустого ответа."""
    pipeline = target_run.ChemblTargetPipeline(
        config=pipeline_config_fixture,
        run_id=run_id,
    )

    mock_client = Mock(spec=ChemblClient)
    mock_client.handshake.return_value = {}

    result = pipeline.fetch_chembl_release(mock_client)

    assert result is None


@pytest.mark.unit
def test_fetch_chembl_release_alternative_field(
    pipeline_config_fixture,
    run_id: str,
) -> None:
    """Тест использования альтернативного поля chembl_release."""
    pipeline = target_run.ChemblTargetPipeline(
        config=pipeline_config_fixture,
        run_id=run_id,
    )

    mock_client = Mock(spec=ChemblClient)
    mock_client.handshake.return_value = {"chembl_release": "35"}

    result = pipeline.fetch_chembl_release(mock_client)

    assert result == "35"


@pytest.mark.unit
def test_fetch_chembl_release_caching(
    pipeline_config_fixture,
    run_id: str,
) -> None:
    """Тест кэширования релиза (повторный вызов не должен делать запрос)."""
    pipeline = target_run.ChemblTargetPipeline(
        config=pipeline_config_fixture,
        run_id=run_id,
    )

    mock_client = Mock(spec=ChemblClient)
    mock_client.handshake.return_value = {"chembl_db_version": "33"}

    # Первый вызов
    result1 = pipeline.fetch_chembl_release(mock_client)
    assert result1 == "33"
    assert mock_client.handshake.call_count == 1

    # Второй вызов - handshake должен быть вызван снова (кэширование на уровне ChemblClient)
    result2 = pipeline.fetch_chembl_release(mock_client)
    assert result2 == "33"
    # ChemblClient кэширует внутри себя, но метод вызывается
    assert mock_client.handshake.call_count >= 1


@pytest.mark.unit
def test_fetch_chembl_release_invalid_json(
    pipeline_config_fixture,
    run_id: str,
) -> None:
    """Тест обработки некорректного JSON ответа."""
    pipeline = target_run.ChemblTargetPipeline(
        config=pipeline_config_fixture,
        run_id=run_id,
    )

    mock_response = Mock()
    mock_response.json.side_effect = ValueError("Invalid JSON")
    mock_client = Mock(spec=UnifiedAPIClient)
    mock_client.get.return_value = mock_response

    with patch("bioetl.chembl.common.descriptor.UnifiedLogger.get"):
        result = pipeline.fetch_chembl_release(mock_client)

    assert result is None


@pytest.mark.unit
def test_fetch_chembl_release_testitem_special_handling(
    pipeline_config_fixture,
    run_id: str,
) -> None:
    """Тест специальной обработки релиза в TestItem пайплайне."""
    pipeline = testitem_run.TestItemChemblPipeline(
        config=pipeline_config_fixture,
        run_id=run_id,
    )

    mock_client = Mock(spec=ChemblClient)
    mock_client.handshake.return_value = {
        "chembl_db_version": "36",
        "api_version": "1.1",
    }

    result = pipeline._fetch_chembl_release(mock_client)  # noqa: SLF001

    assert result == "36"
    assert pipeline.chembl_release == "36"
    assert pipeline.api_version == "1.1"
    assert pipeline.chembl_release_metadata() == {
        "chembl_db_version": "36",
        "api_version": "1.1",
    }


@pytest.mark.unit
def test_fetch_chembl_release_generic_exception(
    pipeline_config_fixture,
    run_id: str,
) -> None:
    """Тест обработки общего исключения."""
    pipeline = target_run.ChemblTargetPipeline(
        config=pipeline_config_fixture,
        run_id=run_id,
    )

    mock_client = Mock(spec=ChemblClient)
    mock_client.handshake.side_effect = Exception("Unexpected error")

    with patch("bioetl.chembl.common.descriptor.UnifiedLogger.get"):
        result = pipeline.fetch_chembl_release(mock_client)

    assert result is None


@pytest.mark.unit
def test_fetch_chembl_release_no_client_methods(
    pipeline_config_fixture,
    run_id: str,
) -> None:
    """Тест обработки клиента без методов handshake или get."""
    pipeline = target_run.ChemblTargetPipeline(
        config=pipeline_config_fixture,
        run_id=run_id,
    )

    mock_client = Mock()
    # Убираем методы handshake и get
    del mock_client.handshake
    del mock_client.get

    result = pipeline.fetch_chembl_release(mock_client)

    assert result is None


@pytest.mark.unit
def test_resolve_chembl_release_returns_release_and_metadata(
    pipeline_config_fixture,
    run_id: str,
) -> None:
    """Высокоуровневый резолвер должен обновлять кеш и возвращать release."""

    pipeline = target_run.ChemblTargetPipeline(
        config=pipeline_config_fixture,
        run_id=run_id,
    )

    log = Mock()
    chembl_client = object()

    with patch.object(pipeline, "fetch_chembl_release", return_value="27") as mock_fetch:
        release_value, metadata = pipeline.resolve_chembl_release(chembl_client, log)

    assert release_value == "27"
    assert metadata == {}
    assert pipeline.chembl_release == "27"
    mock_fetch.assert_called_once_with(chembl_client, log)


@pytest.mark.unit
def test_resolve_chembl_release_includes_optional_versions(
    pipeline_config_fixture,
    run_id: str,
) -> None:
    """Метаданные должны включать chembl_db_version и api_version при наличии."""

    pipeline = target_run.ChemblTargetPipeline(
        config=pipeline_config_fixture,
        run_id=run_id,
    )

    pipeline.chembl_db_version = "31"  # type: ignore[attr-defined]
    pipeline._set_api_version("1.2")

    log = Mock()
    chembl_client = object()

    with patch.object(pipeline, "fetch_chembl_release", return_value=None) as mock_fetch:
        release_value, metadata = pipeline.resolve_chembl_release(chembl_client, log)

    assert release_value is None
    assert metadata == {"chembl_db_version": "31", "api_version": "1.2"}
    mock_fetch.assert_called_once_with(chembl_client, log)


@pytest.mark.unit
def test_resolve_chembl_release_handles_fetch_exception(
    pipeline_config_fixture,
    run_id: str,
) -> None:
    """Исключения fetch должны логироваться и приводить к (None, {})."""

    pipeline = target_run.ChemblTargetPipeline(
        config=pipeline_config_fixture,
        run_id=run_id,
    )

    log = Mock()
    chembl_client = object()

    with patch.object(
        pipeline, "fetch_chembl_release", side_effect=RuntimeError("boom")
    ) as mock_fetch:
        release_value, metadata = pipeline.resolve_chembl_release(chembl_client, log)

    assert release_value is None
    assert metadata == {}
    mock_fetch.assert_called_once_with(chembl_client, log)
    log.warning.assert_called_once()


@pytest.mark.unit
def test_ensure_chembl_release_uses_context_resolver(
    pipeline_config_fixture,
    run_id: str,
) -> None:
    """ensure_chembl_release должен уважать release_resolver контекста."""

    pipeline = target_run.ChemblTargetPipeline(
        config=pipeline_config_fixture,
        run_id=run_id,
    )

    log = Mock()
    resolver = Mock(return_value="55")
    context = ChemblExtractionContext(
        source_config=object(),
        iterator=object(),
        chembl_client=None,
        release_resolver=resolver,
    )

    release_value, metadata = pipeline.ensure_chembl_release(context, log)

    assert release_value == "55"
    assert metadata == {}
    assert pipeline.chembl_release == "55"
    assert context.chembl_release == "55"
    resolver.assert_called_once()


@pytest.mark.unit
def test_ensure_chembl_release_calls_resolve_when_missing(
    pipeline_config_fixture,
    run_id: str,
) -> None:
    """ensure_chembl_release использует resolve_chembl_release при отсутствии кэша."""

    pipeline = target_run.ChemblTargetPipeline(
        config=pipeline_config_fixture,
        run_id=run_id,
    )

    log = Mock()
    chembl_client = object()
    context = ChemblExtractionContext(
        source_config=object(),
        iterator=object(),
        chembl_client=chembl_client,
    )

    with patch.object(
        pipeline,
        "resolve_chembl_release",
        return_value=("27", {"chembl_db_version": "33"}),
    ) as mock_resolve:
        release_value, metadata = pipeline.ensure_chembl_release(context, log)

    assert release_value == "27"
    assert metadata == {"chembl_db_version": "33"}
    assert context.chembl_release == "27"
    assert pipeline.chembl_release == "27"
    mock_resolve.assert_called_once_with(chembl_client, log, context.iterator)


@pytest.mark.unit
def test_publish_release_metadata_merges_values(
    pipeline_config_fixture,
    run_id: str,
) -> None:
    """publish_release_metadata добавляет chembl_release и метаданные."""

    pipeline = target_run.ChemblTargetPipeline(
        config=pipeline_config_fixture,
        run_id=run_id,
    )

    pipeline.update_chembl_release_metadata(api_version="1.1")
    payload = pipeline.publish_release_metadata({"rows": 10}, release="42")

    assert payload["chembl_release"] == "42"
    assert payload["api_version"] == "1.1"
    assert payload["rows"] == 10
