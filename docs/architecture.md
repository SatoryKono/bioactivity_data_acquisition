# Pipeline Architecture

```mermaid
flowchart LR
  A[Config (YAML + ENV + --set)] --> B[CLI Typer команда]
  B --> C[PipelineBase.run]
  C --> D[run_descriptor_extraction]
  D --> E[transform hooks]
  E --> F[validate]
  F --> G[write + QC + metadata]
```

Пайплайн принимает конфигурацию как объединение YAML-файлов, переменных
окружения и флагов `--set`, после чего Typer-команда из `src/bioetl/cli/`
инициирует запуск. Каждый этап реализован отдельными компонентами из
`src/bioetl/pipelines/` и `src/bioetl/core/`, соблюдающими контракт
`extract → transform → validate → output`. ChEMBL-пайплайны используют
дополнительный слой `ChemblPipelineBase`, который инкапсулирует handshake,
подготовку дескрипторов и сбор статистики.

## Публичный API слоя пайплайнов

| Уровень | Модуль | Основные классы/методы | Назначение |
| --- | --- | --- | --- |
| Ядро | `bioetl.pipelines.base` | `PipelineBase.run`, `PipelineBase.write` | Общий жизненный цикл и deterministic I/O. |
| Упрощённый интерфейс | `bioetl.pipelines.unified_base` | `UnifiedPipelineBase`, mixin-слой | Композиция логирования, QC и трансформаций; публичный API для CLI. |
| ChEMBL-надстройка | `bioetl.chembl.common.descriptor` | `ChemblPipelineBase`, `run_descriptor_extraction`, `resolve_chembl_release` | Построение дескрипторов, batched extraction и согласованные метаданные релиза. |
| CLI | `bioetl.cli.cli_app` | команды `activity_chembl`, `document_chembl`, ... | Пользовательские точки входа, делегирующие в `UnifiedPipelineBase`. |

Внешним пользователям доступны следующие сценарии:

1. **Запуск пайплайна** — создать экземпляр конкретного класса (например,
   `ChemblActivityPipeline`) и вызвать `run(output_dir, ...)`. CLI делает то же
   самое, получая конфигурацию из YAML и окружения.
2. **Частичное переиспользование** — вызвать `run_descriptor_extraction()` с
   собственными идентификаторами или фильтрами. Метод возвращает кортеж
   `(dataframe, stats)` и допускает dry-run.
3. **Расширение** — наследоваться от `UnifiedPipelineBase`, переопределяя хуки
   `build_descriptor`, `transform`, `augment_metadata`, `resolve_chembl_release`
   и т.п.

## UnifiedPipelineBase и mixin-слой

`UnifiedPipelineBase` объединяет mixin’ы логирования, пагинации, валидации
`pandera` и генерации артефактов. Все публичные пайплайны ChEMBL наследуются от
этого класса, чтобы получать единый `run()` и набор утилит:

- `prepare_run`/`finalize_run` — опциональные хуки начала/конца.
- `transform` → `validate` — последовательность стадий, которую можно частично
  переопределить (например, `pre_transform`).
- `augment_metadata` — формирует manifest/QC payload на основе статистики,
  собранной `run_descriptor_extraction`.

## ChemblPipelineBase и дескрипторы

ChEMBL-пайплайны описывают сущность через
`ChemblExtractionDescriptor.build_context()`. Базовый класс обеспечивает:

- регистрацию HTTP-клиентов и фабрик сущностей (assay/activity/...);
- вызов `resolve_chembl_release` и логирование информации о релизе;
- dry-run и batched fetcher с накоплением `BatchExtractionStats`;
- унифицированный `extract_all()` для CLI команд без списка идентификаторов.

`run_descriptor_extraction` остаётся единственной тяжёлой точкой, которую
переиспользуют все пять ChEMBL-пайплайнов. Параметры (`summary_event`,
`metadata_filters`, `id_normalizer`, `summary_extra_factory`, `fetch_mode`)
должны передаваться из дочернего класса при необходимости.

## Релизы ChEMBL и метаданные

`resolve_chembl_release` объединяет фактический релиз, возвращаемый клиентом,
и дополнительные поля (`api_version`, `chembl_db_version`) в metadata-пэйлоад,
который попадает в QC и manifest. При dry-run релиз всё равно считывается, чтобы
журналы и отчёты оставались сопоставимыми с боевыми запусками.

## Связанные разделы

- [Обзор CLI](cli/00-cli-overview.md)
- [Каталог пайплайнов](pipelines/10-pipelines-catalog.md)
- [Политика детерминизма](determinism/01-determinism-policy.md)

## Дальнейшее чтение

- [Архитектура ChEMBL-пайплайнов](pipelines/chembl/00-architecture.md)
- [План унификации интерфейсов](chembl_pipeline_interface.md)
