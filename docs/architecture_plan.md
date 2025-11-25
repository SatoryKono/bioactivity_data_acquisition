# Обновлённый архитектурный план для ETL-пайплайнов bioetl

## Ключевые наблюдения из документации
- Пайплайны должны следовать контракту `extract → transform → validate → write` и использовать абстракции `PipelineBase`, `StageABC`, `PipelineHookABC`, гарантируя изоляцию стадий и централизованное закрытие ресурсов. [docs/pipeline_objects_interaction.md]
- Структура каталогов фиксирована по функциональным слоям (`pipeline/`, `source/`, `processing/`, `validation/`, `output/`, `utils/`), что позволяет разделять оркестрацию, работу с источником, трансформацию, валидацию и запись. [docs/pipeline_structure.md]
- Стайлгайд требует одного публичного пайплайна на источник, именование `{entity}_{source}`, использование адаптеров к внешним системам, звёздную схему (dimensions + fact) и унифицированные компоненты (`UnifiedLogger`, `UnifiedAPIClient`, схемы). [docs/styleguide/08-etl-architecture.md]

## Обновлённая целевая доменная модель
- **Размерности**: `documents_dim`, `targets_dim`, `assays_dim`, `testitems_dim` — ответственны за справочные сущности, нормализацию и обогащение метаданными.
- **Факт**: `activity_fact` хранит измерения с FK на размерности; бизнес-ключи и хэши используются для идемпотентности и дедупликации.
- **Инварианты**: единый бизнес-ключ на сущность, детерминированные хэши по записям, валидируемые Pandera-схемами; строгие ограничения типов (например, выравнивание tax_id), контроль качества через `ValidationResult` и `DQIssue`.

## Слойные границы и адаптеры
- **pipeline/**: оркестрация через `PipelineBase.run`, хуки для логов/метрик, CLI-команды на `CLICommandABC` с конфиг-резолвером и секретами.
- **source/**: `RequestBuilderABC` + `PaginatorABC` генерируют запросы; `SourceClientABC` с `RateLimiterABC` и `RetryPolicyABC` выполняет HTTP; `ResponseParserABC` формирует поток записей. Все источники подключаются через адаптеры (`ChEMBLAdapter`, `PubChemAdapter` и т.п.).
- **processing/**: детерминированные трансформации в `TransformerABC`; обогащение через `SideInputProviderABC` + `LookupEnricherABC`; дедупликация и консолидация с `BusinessKeyDeriverABC`, `DeduplicatorABC`, `MergeStrategyABC`, `HasherABC`.
- **validation/**: `SchemaProviderABC` отдаёт Pandera-схемы; `ValidatorABC` и DQ-правила фиксируют ошибки и статистику; результаты передаются `ProgressReporterABC`.
- **output/**: `PathStrategyABC` строит пути; `WriterABC` выполняет атомарную запись CSV с сортировкой; `MetadataWriterABC` сохраняет метаинформацию запуска.
- **utils/**: конфиг, секреты, кэш, логирование и трейсинг через унифицированные адаптеры, скрывая инфраструктурные детали от доменной логики.

## План реализации
1. **Фиксация схем и доменных классов**: выровнять типы ключевых полей (tax_id и др.) в Pandera-схемах размерностей и фактов; добавить бизнес-ключи и хэши в факт-таблицу.
2. **Источник и адаптеры**: реализовать адаптеры на базе `SourceClientABC` для ChEMBL/PubChem/UniProt, используя `RequestBuilderABC`, `PaginatorABC`, `ResponseParserABC`, `RetryPolicyABC` и `RateLimiterABC`.
3. **Трансформация и обогащение**: собрать пайплайн `StageABC` для нормализации, lookup-обогащения и дедупликации с конфигурируемыми стратегиями merge.
4. **Валидация и DQ**: подключить `SchemaProviderABC` к Pandera-схемам, реализовать `ValidatorABC` и агрегатные `DQRuleABC`, интегрировать отчёты `ValidationResult`/`DQIssue` в прогресс-репортинг.
5. **Запись и метаданные**: использовать `PathStrategyABC` для детерминированных путей, `WriterABC` для атомарной записи CSV и `MetadataWriterABC` для хэшированных метаданных запуска.
6. **CLI и конфиг**: внедрить `CLICommandABC` с `ConfigResolverABC`/`SecretProviderABC`, поддержать именование `{entity}_{source}` и единый контракт CLI для запуска стадий и dry-run.
7. **Тестирование и наблюдаемость**: написать pytest-тесты для адаптеров, трансформаций и схем; включить структурированное логирование через `LoggerAdapterABC` и трассировку `TracerABC` в хуках пайплайна.

## Допущения
- Pandas/Pandera остаются основой для DataFrame-валидации и детерминированной записи.
- Вычисление бизнес-ключей и хэшей делается в processing-слое и не зависит от конкретной СУБД.
- Конфигурация и секреты поставляются через YAML + переменные окружения, поддерживаемые `ConfigResolverABC` и `SecretProviderABC`.
