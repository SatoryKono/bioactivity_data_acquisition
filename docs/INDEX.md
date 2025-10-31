# Сводная документация BioETL

## навигация

- [Архитектура](requirements/00-architecture-overview.md)
- [Требования к данным и источникам](requirements/03-data-sources-and-spec.md)
- [Пайплайны и шаги](pipelines/PIPELINES.md)
- [Конфигурации и профили](configs/CONFIGS.md)
- [CLI и операционные сценарии](cli/CLI.md)
- [Качество, тесты и QC](qc/QA.md)
- [Политика именования артефактов](requirements/NAMING.md)

## структура-документации

Архивные материалы прошлых фаз перенесены в `docs/archive/`. Все действующие требования и публичные контракты собраны в файлах выше. Для навигации по полному перечню компонентов используйте автоматический инвентаризационный снапшот `docs/requirements/PIPELINES.inventory.csv`.

## глоссарий

| Термин | Определение |
| --- | --- |
| **Pipeline** | Компонент, реализующий цепочку `extract → normalize → validate → write`, основанную на `PipelineBase` ([ref: repo:src/bioetl/pipelines/base.py@test_refactoring_32]). |
| **UnifiedAPIClient** | Унифицированный HTTP-клиент с ретраями, троттлингом и fallback-стратегиями ([ref: repo:src/bioetl/core/api_client.py@test_refactoring_32]). |
| **UnifiedOutputWriter** | Подсистема детерминированной фиксации артефактов и QC ([ref: repo:src/bioetl/core/output_writer.py@test_refactoring_32]). |
| **Schema Registry** | Реестр Pandera-схем, определяющих допустимые столбцы и типы ([ref: repo:src/bioetl/schemas/registry.py@test_refactoring_32]). |
| **Business Key** | Минимальный набор колонок, однозначно идентифицирующий запись; фиксируется для каждой сущности в `03-data-sources-and-spec.md`. |

## контроль-обновлений

- Любое изменение публичного контракта или схемы **MUST** сопровождаться записью в `CHANGELOG.md` и обновлением соответствующих требований.
- Документы **MUST** соблюдать политику именования ([requirements/NAMING.md](requirements/NAMING.md)).
- Пул-реквесты с документационными изменениями **SHOULD** запускать локальный линк-чекер и markdown-линт (`make docs-verify`).


