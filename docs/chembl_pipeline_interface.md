# Унифицированный интерфейс ChEMBL-пайплайнов

Документ описывает фактический контракт, которым пользуются все ChEMBL-пайплайны
(`activity`, `assay`, `document`, `target`, `testitem`). Он заменяет предыдущий
план миграции и фиксирует конечную архитектуру базовых классов и миксинов.

## Сводка

1. Каждый пайплайн наследуется от `bioetl.pipelines.unified_base.UnifiedPipelineBase`,
   который композитно соединяет `ChemblPipelineBase` и набор миксинов
   (`LoggingMixin`, `ReleaseHandshakeMixin`, `PaginatedExtractorMixin`,
   `SchemaValidationMixin`, `RecordNormalizationMixin`, `NestedSerializerMixin`,
   `BatchIdExtractionMixin`, `TransformMixin`, `IOArtifactsMixin`). Этот стек
   обеспечивает единый жизненный цикл `extract → transform → validate → write`.
2. `bioetl.chembl.common.descriptor.ChemblPipelineBase` отвечает за построение
   дескриптора (`build_descriptor`), управление `run_descriptor_extraction`,
   обработку dry-run режимов и объединение метаданных (`record_extract_metadata`).
3. Миксины предоставляют независимые контракты: логирование стадий, handshake с
   API ChEMBL, нормализацию идентификаторов, сериализацию вложенных полей,
   батчевое извлечение по ID и детерминированную запись артефактов.
4. CLI (`bioetl.cli.cli_app`) взаимодействует только с публичными методами
   `run`, `build_descriptor`, `transform`/`validate`, `augment_metadata` и
   `run_descriptor_extraction`. Это позволяет расширять стек без изменения
   пользовательского API.
5. Добавление нового ChEMBL-пайплайна сводится к реализации `build_descriptor`,
   опциональных хуков `pre_transform/domain_enrich/post_transform`, а также
   предоставлению специфичных правил нормализации/валидации через миксины.

## Базовые классы

### `ChemblPipelineBase`
- Расположен в `src/bioetl/chembl/common/descriptor.py`.
- Реализует загрузку конфигурации, фабрики HTTP-клиентов, резолверы схем,
  определяет контракт `build_descriptor`, `extract_all`, `extract_by_ids` и
  `resolve_legacy_extract_ids`.
- Метод `extract` автоматически выбирает режим (полный или выборочный) и
  публикует telemetry через `extract_event_name`.
- `record_extract_metadata` и `augment_metadata` объединяют данные для
  `meta.yaml`, включая release, фильтры и пользовательские ключи.

### `UnifiedPipelineBase`
- Живёт в `src/bioetl/pipelines/unified_base.py` и наследует весь стек миксинов.
- Переопределяет `run`, чтобы проксировать флаги CLI (`--extended`,
  `--include-correlation`, `--qc-thresholds`) без изменения публичного контракта
  `PipelineBase.run`.
- Предоставляет хуки `prepare_run`/`finalize_run`, которые запускаются вокруг
  стандартного ETL-процесса и доступны наследникам без ручного переопределения
  `run`.

### `PipelineBase`
- Общий шаблон `extract → transform → validate → write`.
- Содержит реализацию `write`, `build_qc_metrics`, `build_correlation_report`,
  `augment_metadata` и финализацию ресурсов.
- Модули CLI вызывают именно `PipelineBase.run`, поэтому любые расширения должны
  соблюдать его контракт (возврат `RunResult`).

## Стек миксинов

| Mixin | Роль |
| --- | --- |
| `LoggingMixin` | Единый `stage_logger` и `logger_for` с измерением длительностей стадий. |
| `ReleaseHandshakeMixin` | Кэширует handshake с `/status` ЧЕМБЛ, записывает release и версию API в метаданные. |
| `PaginatedExtractorMixin` | Стандартизирует обработку пагинации, логирование страниц и событий `on_page`. |
| `SchemaValidationMixin` | Добавляет загрузку схемы Pandera и logging вокруг `validate`. |
| `RecordNormalizationMixin` | Переиспользуемая нормализация идентификаторов/строк с отчётами в лог. |
| `NestedSerializerMixin` | Детерминированно сериализует вложенные структуры в string-столбцы. |
| `BatchIdExtractionMixin` | Общая реализация `extract_by_ids`, включая планирование чанков, фабрики fetcher’ов и finalize-хуки. |
| `TransformMixin` | Реализует стандартный `transform` как цепочку `pre_transform` → schema enforcement → `domain_enrich` → `post_transform`. |
| `IOArtifactsMixin` | Связывает `save_results` с `PipelineBase.write`, сохраняя поддержку `extended`, correlation report и QC. |

## `PipelineStagesProtocol` и фабрика стадий

- Все ChEMBL-пайплайны реализуют `PipelineStagesProtocol` из
  `src/bioetl/pipelines/base.py`. Контракт гарантирует, что `StageFactory`
  построит детерминированный план стадий и вызовет
  `prepare_run`/`finalize_run` вокруг `extract → transform → validate → write →
  cleanup`.
- `PipelineBase.create_stage_factory()` возвращает фабрику по умолчанию с пятью
  `PipelineStageCommand`. При необходимости можно вернуть собственный класс,
  добавляющий промежуточные шаги или отключающий стадию. Поведение покрыто в
  `tests/pipelines/test_pipeline_commands.py`.
- Для CLI-команд ChEMBL предназначен `src/bioetl/pipelines/chembl/stage_runner.py`:
  он регистрирует пайплайны, проверяет контракт и умеет строить частичные планы
  (`extract`+`transform`, `validate` отдельно и т.д.). Примеры использования и
  негативные сценарии описаны в `tests/bioetl/pipelines/chembl/test_stage_runner.py`.

## Контракт извлечения

1. Дочерние классы реализуют `build_descriptor`, возвращающий
   `ChemblExtractionDescriptor`. Дескриптор описывает источник (`source_name`),
   обязательные поля, фабрики fetcher’ов и контекста.
2. `ChemblPipelineBase.extract` автоматически решает, запускать ли
   `extract_all` (полная выгрузка) или `BatchIdExtractionMixin.extract_by_ids`.
   Последний использует `BatchIdExtractionPlan`, который вычисляет параметры
   `batch_size`, `chunk_size`, `select_fields`, `metadata_filters` и dry-run
   обработчики, а затем вызывает `run_descriptor_extraction`.
3. `BatchIdExtractionMixin` предоставляет набор методов `id_extraction_*`,
   позволяющих переопределять только необходимые части плана: выбор полей,
   лимиты, нормализаторы идентификаторов, фабрики finalize-хуков, сортировку и
   обработку пустых фреймов. После выполнения `post_id_extraction` даёт возможность
   наблюдать статистику перед трансформацией.
4. Метаданные извлечения автоматически передаются в `record_extract_metadata`,
   поэтому CLI и QC получают единый `meta.yaml` независимо от режима.

## Контракт трансформаций и валидации

- `TransformMixin.transform` выполняет копию фрейма, вызывает `pre_transform`,
  после чего применяет `_normalize_and_enforce_schema` из `ChemblPipelineBase` и
  `RecordNormalizationMixin`, гарантируя соблюдение порядка колонок и правил
  нормализации.
- `domain_enrich` и `post_transform` позволяют добавлять бизнес-логику между
  нормализацией и финальными проверками.
- `SchemaValidationMixin.validate` оборачивает `PipelineBase.validate` в логгер
  стадии и загружает схему через `config.validation.schema_out`.

## Стратегии батчинга и валидации

- `BatchIdExtractionPlan` агрегирует настройки батчей (ID чанки, `batch_size`,
  `chunk_size`, `select_fields`, фабрики fetcher’ов, `metadata_filters`,
  `post_id_extraction`). Пайплайны могут переопределить конкретные методы
  (`id_chunk_size_cap`, `id_extraction_postprocess`, `post_id_extraction`), не
  меняя остальной стек.
- Валидация управляется стратегиями из `src/bioetl/pipelines/validation.py`:
  `StrictValidation` выбрасывает исключение при первом нарушении, а
  `FailOpenValidation` логирует ошибки и продолжает выполнение, добавляя
  диагностику в `RunResult`. Выбор стратегии зависит от CLI (`--fail-on-qc-violation`)
  и `config.validation.allow_schema_migration`.

## Контракт метаданных и handshake

- `ReleaseHandshakeMixin.perform_handshake` обращается к клиенту ChEMBL и
  складывает release/api_version в `record_extract_metadata`, что автоматически
  попадает в `augment_metadata` и `meta.yaml`.
- `PipelineBase.augment_metadata` объединяет `BatchExtractionStats.metadata` с
  ранее записанными данными об источнике, фильтрах и временных метках. При
  необходимости дочерние классы переопределяют метод, чтобы добавить
  специфические ключи (например, `chembl_db_version`).
- Вызовы `save_results` из `IOArtifactsMixin` гарантируют детерминированную
  запись CSV и побочных отчётов, после чего `RunResult` возвращает `write_result`
  для CLI.

## Как расширять пайплайн

1. Наследуйтесь от `UnifiedPipelineBase` и определите `actor`, `pipeline_code`,
   `id_column`, `extract_event_name`.
2. Реализуйте `build_descriptor` с фабриками клиента и fetcher’ов.
3. При необходимости переопределите `identifier_rules`, `string_rules`,
   `nested_column_specs`, `pre_transform/domain_enrich/post_transform`.
4. Если нужны дополнительные поля в manifest/QC — переопределите
   `augment_metadata`, вызывая `super()` и добавляя свои ключи.
5. Покройте пайплайн тестами, используя `BatchIdExtractionMixin.id_chunk_size_cap`
   для ускорения smoke-сценариев.
