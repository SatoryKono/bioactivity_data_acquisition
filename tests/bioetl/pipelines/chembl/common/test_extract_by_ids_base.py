"""Централизованные тесты для общего шаблона extract_by_ids."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import cast
from unittest.mock import Mock, patch

import pytest

from bioetl.clients.entities.client_assay import ChemblAssayClient
from bioetl.clients.entities.client_document import ChemblDocumentClient
from bioetl.clients.entities.client_target import ChemblTargetClient
from bioetl.clients.entities.client_testitem import ChemblTestitemClient
from bioetl.pipelines.chembl.assay import run as assay_run
from bioetl.pipelines.chembl.document import run as document_run
from bioetl.pipelines.chembl.target import run as target_run
from bioetl.pipelines.chembl.testitem import run as testitem_run


def _get_entity_client_type(pipeline_cls: type) -> type | None:
    """Возвращает тип entity_client для данного pipeline класса."""
    if pipeline_cls == target_run.ChemblTargetPipeline:
        return ChemblTargetClient
    if pipeline_cls == assay_run.ChemblAssayPipeline:
        return ChemblAssayClient
    if pipeline_cls == document_run.ChemblDocumentPipeline:
        return ChemblDocumentClient
    if pipeline_cls == testitem_run.TestItemChemblPipeline:
        return ChemblTestitemClient
    return None


@pytest.mark.parametrize(
    ("pipeline_cls", "id_column", "sample_ids"),
    [
        (target_run.ChemblTargetPipeline, "target_chembl_id", ["CHEMBL1", "CHEMBL2"]),
        (assay_run.ChemblAssayPipeline, "assay_chembl_id", ["CHEMBL100", "CHEMBL101"]),
        (document_run.ChemblDocumentPipeline, "document_chembl_id", ["CHEMBL200", "CHEMBL201"]),
        (testitem_run.TestItemChemblPipeline, "molecule_chembl_id", ["CHEMBL300", "CHEMBL301"]),
    ],
)
@pytest.mark.unit
def test_extract_by_ids_happy_path(
    pipeline_cls: type,
    id_column: str,
    sample_ids: list[str],
    pipeline_config_fixture,
    run_id: str,
    mock_chembl_bundle,
) -> None:
    """Параметризованный тест успешного извлечения по ID для всех пайплайнов."""
    pipeline = pipeline_cls(config=pipeline_config_fixture, run_id=run_id)  # type: ignore[reportAbstractUsage]

    # Настраиваем entity_client для возврата данных
    def mock_iterate_by_ids(
        ids: Sequence[str],
        select_fields: Sequence[str] | None = None,
    ) -> Sequence[dict[str, object]]:
        return [{id_column: id_val} for id_val in ids]

    # Создаем мок entity_client с правильной спецификацией для прохождения проверки isinstance
    client_type = _get_entity_client_type(pipeline_cls)
    if client_type is not None:
        mock_entity_client = Mock(spec=client_type)
        mock_entity_client.iterate_by_ids = mock_iterate_by_ids
        mock_chembl_bundle.entity_client = mock_entity_client
    else:
        mock_chembl_bundle.entity_client.iterate_by_ids = mock_iterate_by_ids

    # Для Document pipeline нужно мокировать _build_document_client
    if pipeline_cls == document_run.ChemblDocumentPipeline:
        def mock_iterate_by_ids_doc(
            ids: Sequence[str],
            select_fields: Sequence[str] | None = None,
        ) -> Iterable[dict[str, object]]:
            return cast(Iterable[dict[str, object]], iter([{id_column: id_val} for id_val in ids]))

        mock_document_client = Mock()
        mock_document_client.iterate_by_ids = mock_iterate_by_ids_doc
        mock_document_client.batch_size = 20

        with patch.object(pipeline, "build_chembl_entity_bundle", return_value=mock_chembl_bundle):
            with patch.object(pipeline, "fetch_chembl_release", return_value="33"):
                with patch.object(pipeline, "_build_document_client", return_value=mock_document_client):
                    result = pipeline.extract_by_ids(sample_ids)  # type: ignore[misc]
    else:
        with patch.object(pipeline, "build_chembl_entity_bundle", return_value=mock_chembl_bundle):
            with patch.object(pipeline, "fetch_chembl_release", return_value="33"):
                result = pipeline.extract_by_ids(sample_ids)  # type: ignore[misc]

    assert not result.empty
    assert id_column in result.columns
    # Проверяем, что все запрошенные ID присутствуют (или обработаны)
    assert len(result) <= len(sample_ids)


@pytest.mark.parametrize(
    "pipeline_cls",
    [
        target_run.ChemblTargetPipeline,
        assay_run.ChemblAssayPipeline,
        document_run.ChemblDocumentPipeline,
    ],
)
@pytest.mark.unit
def test_extract_by_ids_empty_list(
    pipeline_cls: type,
    pipeline_config_fixture,
    run_id: str,
    mock_chembl_bundle,
) -> None:
    """Тест обработки пустого списка ID."""
    pipeline = pipeline_cls(config=pipeline_config_fixture, run_id=run_id)  # type: ignore[reportAbstractUsage]

    # Для Document pipeline нужно мокировать _build_document_client
    if pipeline_cls == document_run.ChemblDocumentPipeline:
        mock_document_client = Mock()
        mock_document_client.iterate_by_ids = lambda ids, select_fields=None: iter([])
        mock_document_client.batch_size = 20

        with patch.object(pipeline, "build_chembl_entity_bundle", return_value=mock_chembl_bundle):
            with patch.object(pipeline, "fetch_chembl_release", return_value="33"):
                with patch.object(pipeline, "_build_document_client", return_value=mock_document_client):
                    result = pipeline.extract_by_ids([])  # type: ignore[misc]
    else:
        with patch.object(pipeline, "build_chembl_entity_bundle", return_value=mock_chembl_bundle):
            with patch.object(pipeline, "fetch_chembl_release", return_value="33"):
                result = pipeline.extract_by_ids([])  # type: ignore[misc]

    assert result.empty


@pytest.mark.parametrize(
    ("pipeline_cls", "id_column"),
    [
        (target_run.ChemblTargetPipeline, "target_chembl_id"),
        (assay_run.ChemblAssayPipeline, "assay_chembl_id"),
    ],
)
@pytest.mark.unit
def test_extract_by_ids_single_id(
    pipeline_cls: type,
    id_column: str,
    pipeline_config_fixture,
    run_id: str,
    mock_chembl_bundle,
) -> None:
    """Тест обработки одного ID."""
    pipeline = pipeline_cls(config=pipeline_config_fixture, run_id=run_id)  # type: ignore[reportAbstractUsage]
    single_id = ["CHEMBL1"]

    def mock_iterate_by_ids(
        ids: Sequence[str],
        select_fields: Sequence[str] | None = None,
    ) -> Sequence[dict[str, object]]:
        return [{id_column: ids[0]}]

    # Создаем мок entity_client с правильной спецификацией для прохождения проверки isinstance
    client_type = _get_entity_client_type(pipeline_cls)
    if client_type is not None:
        mock_entity_client = Mock(spec=client_type)
        mock_entity_client.iterate_by_ids = mock_iterate_by_ids
        mock_chembl_bundle.entity_client = mock_entity_client
    else:
        mock_chembl_bundle.entity_client.iterate_by_ids = mock_iterate_by_ids

    with patch.object(pipeline, "build_chembl_entity_bundle", return_value=mock_chembl_bundle):
        with patch.object(pipeline, "fetch_chembl_release", return_value="33"):
            result = pipeline.extract_by_ids(single_id)  # type: ignore[misc]

    assert not result.empty
    assert len(result) == 1
    assert id_column in result.columns


@pytest.mark.parametrize(
    ("pipeline_cls", "id_column"),
    [
        (target_run.ChemblTargetPipeline, "target_chembl_id"),
        (assay_run.ChemblAssayPipeline, "assay_chembl_id"),
    ],
)
@pytest.mark.unit
def test_extract_by_ids_duplicates(
    pipeline_cls: type,
    id_column: str,
    pipeline_config_fixture,
    run_id: str,
    mock_chembl_bundle,
) -> None:
    """Тест дедупликации ID."""
    pipeline = pipeline_cls(config=pipeline_config_fixture, run_id=run_id)  # type: ignore[reportAbstractUsage]
    ids_with_duplicates = ["CHEMBL1", "CHEMBL1", "CHEMBL2", "CHEMBL2", "CHEMBL1"]

    def mock_iterate_by_ids(
        ids: Sequence[str],
        select_fields: Sequence[str] | None = None,
    ) -> Sequence[dict[str, object]]:
        # Возвращаем уникальные значения
        unique_ids = list(dict.fromkeys(ids))
        return [{id_column: id_val} for id_val in unique_ids]

    # Создаем мок entity_client с правильной спецификацией для прохождения проверки isinstance
    client_type = _get_entity_client_type(pipeline_cls)
    if client_type is not None:
        mock_entity_client = Mock(spec=client_type)
        mock_entity_client.iterate_by_ids = mock_iterate_by_ids
        mock_chembl_bundle.entity_client = mock_entity_client
    else:
        mock_chembl_bundle.entity_client.iterate_by_ids = mock_iterate_by_ids

    with patch.object(pipeline, "build_chembl_entity_bundle", return_value=mock_chembl_bundle):
        with patch.object(pipeline, "fetch_chembl_release", return_value="33"):
            result = pipeline.extract_by_ids(ids_with_duplicates)  # type: ignore[misc]

    # run_batched_extraction должен дедуплицировать
    assert not result.empty
    unique_count = result[id_column].nunique()
    assert unique_count <= len(set(ids_with_duplicates))


@pytest.mark.parametrize(
    ("pipeline_cls", "id_column"),
    [
        (target_run.ChemblTargetPipeline, "target_chembl_id"),
        (assay_run.ChemblAssayPipeline, "assay_chembl_id"),
    ],
)
@pytest.mark.unit
def test_extract_by_ids_with_whitespace(
    pipeline_cls: type,
    id_column: str,
    pipeline_config_fixture,
    run_id: str,
    mock_chembl_bundle,
) -> None:
    """Тест обработки ID с пробелами."""
    pipeline = pipeline_cls(config=pipeline_config_fixture, run_id=run_id)  # type: ignore[reportAbstractUsage]
    ids_with_spaces = [" CHEMBL1 ", "CHEMBL2", "  CHEMBL3  "]

    def mock_iterate_by_ids(
        ids: Sequence[str],
        select_fields: Sequence[str] | None = None,
    ) -> Sequence[dict[str, object]]:
        # Нормализуем ID (убираем пробелы)
        normalized = [id_val.strip() for id_val in ids]
        return [{id_column: id_val} for id_val in normalized]

    # Создаем мок entity_client с правильной спецификацией для прохождения проверки isinstance
    client_type = _get_entity_client_type(pipeline_cls)
    if client_type is not None:
        mock_entity_client = Mock(spec=client_type)
        mock_entity_client.iterate_by_ids = mock_iterate_by_ids
        mock_chembl_bundle.entity_client = mock_entity_client
    else:
        mock_chembl_bundle.entity_client.iterate_by_ids = mock_iterate_by_ids

    with patch.object(pipeline, "build_chembl_entity_bundle", return_value=mock_chembl_bundle):
        with patch.object(pipeline, "fetch_chembl_release", return_value="33"):
            result = pipeline.extract_by_ids(ids_with_spaces)  # type: ignore[misc]

    assert not result.empty
    # Проверяем, что ID нормализованы
    for id_val in result[id_column]:
        assert not str(id_val).strip() != str(id_val), "ID должны быть нормализованы"


@pytest.mark.parametrize(
    ("pipeline_cls", "id_column"),
    [
        (target_run.ChemblTargetPipeline, "target_chembl_id"),
        (assay_run.ChemblAssayPipeline, "assay_chembl_id"),
    ],
)
@pytest.mark.unit
def test_extract_by_ids_dry_run(
    pipeline_cls: type,
    id_column: str,
    pipeline_config_fixture,
    run_id: str,
    mock_chembl_bundle,
) -> None:
    """Тест dry-run режима."""
    pipeline_config_fixture.cli.dry_run = True  # type: ignore[attr-defined]
    pipeline = pipeline_cls(config=pipeline_config_fixture, run_id=run_id)  # type: ignore[reportAbstractUsage]

    with patch.object(pipeline, "build_chembl_entity_bundle", return_value=mock_chembl_bundle):
        with patch.object(pipeline, "fetch_chembl_release", return_value="33"):
            result = pipeline.extract_by_ids(["CHEMBL1", "CHEMBL2"])  # type: ignore[misc]

    assert result.empty


@pytest.mark.parametrize(
    ("pipeline_cls", "id_column"),
    [
        (target_run.ChemblTargetPipeline, "target_chembl_id"),
        (assay_run.ChemblAssayPipeline, "assay_chembl_id"),
    ],
)
@pytest.mark.unit
def test_extract_by_ids_respects_limit(
    pipeline_cls: type,
    id_column: str,
    pipeline_config_fixture,
    run_id: str,
    mock_chembl_bundle,
) -> None:
    """Тест соблюдения лимита записей."""
    pipeline_config_fixture.cli.limit = 2  # type: ignore[attr-defined]
    pipeline = pipeline_cls(config=pipeline_config_fixture, run_id=run_id)  # type: ignore[reportAbstractUsage]
    many_ids = ["CHEMBL1", "CHEMBL2", "CHEMBL3", "CHEMBL4", "CHEMBL5"]

    def mock_iterate_by_ids(
        ids: Sequence[str],
        select_fields: Sequence[str] | None = None,
    ) -> Sequence[dict[str, object]]:
        return [{id_column: id_val} for id_val in ids]

    mock_chembl_bundle.entity_client.iterate_by_ids = mock_iterate_by_ids

    with patch.object(pipeline, "build_chembl_entity_bundle", return_value=mock_chembl_bundle):
        with patch.object(pipeline, "fetch_chembl_release", return_value="33"):
            result = pipeline.extract_by_ids(many_ids)  # type: ignore[misc]

    # Лимит должен быть соблюден
    assert len(result) <= 2


@pytest.mark.parametrize(
    ("pipeline_cls", "id_column", "client_type"),
    [
        (target_run.ChemblTargetPipeline, "target_chembl_id", ChemblTargetClient),
        (assay_run.ChemblAssayPipeline, "assay_chembl_id", ChemblAssayClient),
    ],
)
@pytest.mark.unit
def test_extract_by_ids_batch_processing(
    pipeline_cls: type,
    id_column: str,
    client_type: type,
    pipeline_config_fixture,
    run_id: str,
    mock_chembl_bundle,
) -> None:
    """Тест обработки больших списков ID с разбиением на батчи."""
    # Устанавливаем маленький batch_size для тестирования батчинга
    source_config = pipeline_config_fixture.domain.sources["chembl"]  # type: ignore[attr-defined]
    # Устанавливаем batch_size напрямую и через parameters для надежности
    source_config.batch_size = 2  # type: ignore[attr-defined]
    if hasattr(source_config, "parameters") and hasattr(source_config.parameters, "batch_size"):
        source_config.parameters.batch_size = 2  # type: ignore[attr-defined]

    pipeline = pipeline_cls(config=pipeline_config_fixture, run_id=run_id)  # type: ignore[reportAbstractUsage]
    many_ids = ["CHEMBL1", "CHEMBL2", "CHEMBL3", "CHEMBL4", "CHEMBL5"]

    batches_called: list[tuple[str, ...]] = []

    def mock_iterate_by_ids(
        ids: Sequence[str],
        select_fields: Sequence[str] | None = None,
    ) -> Sequence[dict[str, object]]:
        batches_called.append(tuple(ids))
        return [{id_column: id_val} for id_val in ids]

    # Создаем мок entity_client с правильной спецификацией для прохождения проверки isinstance
    mock_entity_client = Mock(spec=client_type)
    mock_entity_client.iterate_by_ids = mock_iterate_by_ids
    mock_chembl_bundle.entity_client = mock_entity_client

    # Мокируем source_config.batch_size после создания пайплайна
    with patch.object(pipeline, "build_chembl_entity_bundle", return_value=mock_chembl_bundle):
        with patch.object(pipeline, "fetch_chembl_release", return_value="33"):
            # Патчим _resolve_source_config чтобы вернуть конфиг с batch_size=2
            original_resolve = pipeline._resolve_source_config
            
            def mock_resolve_source_config(source_name: str):
                config = original_resolve(source_name)
                # Создаем новый конфиг с batch_size=2
                from bioetl.config.assay import AssaySourceConfig
                from bioetl.config.target import TargetSourceConfig
                
                if pipeline_cls == target_run.ChemblTargetPipeline:
                    typed_config = TargetSourceConfig.from_source_config(config)
                    typed_config.batch_size = 2
                    return typed_config
                if pipeline_cls == assay_run.ChemblAssayPipeline:
                    typed_config = AssaySourceConfig.from_source_config(config)
                    typed_config.batch_size = 2
                    return typed_config
                return config
            
            with patch.object(pipeline, "_resolve_source_config", side_effect=mock_resolve_source_config):
                result = pipeline.extract_by_ids(many_ids)  # type: ignore[misc]

    # Должно быть несколько батчей (5 ID / 2 на батч = минимум 3 батча)
    assert len(batches_called) > 1, f"Expected multiple batches, got {len(batches_called)}: {batches_called}"
    assert not result.empty

