# VALIDATION_RULES.md

Документ описывает требования к валидации данных для каталога `src/bioetl/sources/<source>/schema/` и метода `validate()` в `src/bioetl/sources/<source>/pipeline.py`. Правила основаны на контракте `PipelineBase` и Pandera-схемах.

## Общие принципы (MUST)

- Каждое `validate()` обязано вызывать `_validate_with_schema` из базового класса и передавать Pandera-схему источника, чтобы обеспечить трассируемые отчёты и единую обработку ошибок. [ref: repo:src/bioetl/pipelines/base.py@test_refactoring_32]
- Перед Pandera-проверкой датафрейм приводится к каноническому порядку столбцов и заполняет отсутствующие обязательные поля безопасными значениями (`Int64`, `float64`, `pd.NA`). [ref: repo:src/bioetl/pipelines/activity.py@test_refactoring_32]
- QC-метрики (fallback.count, fallback.rate и др.) регистрируются вместе со статусом схемы; превышение порогов трактуется как ошибка высокого приоритета. [ref: repo:src/bioetl/pipelines/activity.py@test_refactoring_32]

## Activity (`src/bioetl/sources/activity/schema/`)

- Валидация добавляет отсутствующие колонки по `ActivitySchema` и принудительно приводит `retry_after`, `Int64` и `float64` поля к совместимым типам до передачи в Pandera. [ref: repo:src/bioetl/pipelines/activity.py@test_refactoring_32]
- В отчёт QC входят частота fallback-записей и их причины; при превышении порога `qc_threshold_exceeded` инициируется исключение. [ref: repo:src/bioetl/pipelines/activity.py@test_refactoring_32]
- При фатальной ошибке Pandera сохраняет `failure_cases` и группирует их по колонкам для дальнейшего анализа. [ref: repo:src/bioetl/pipelines/activity.py@test_refactoring_32]

## Assay (`src/bioetl/sources/assay/schema/`)

- Nullable-интегральные колонки приводятся через `coerce_nullable_int`, после чего вызывается Pandera-валидация `AssaySchema`. [ref: repo:src/bioetl/pipelines/assay.py@test_refactoring_32]
- Валидация проверяет согласованность расширенных строк (parameters, classifications) и фиксирует предупреждения по потерянным связям enrichment. [ref: repo:src/bioetl/pipelines/assay.py@test_refactoring_32]
- Итоговый отчёт добавляет QC для количества ассай-классов и таргетов, что облегчает автоматические алерты. [ref: repo:src/bioetl/pipelines/assay.py@test_refactoring_32]

## Document (`src/bioetl/sources/document/schema/`)

- Метод `validate()` гарантирует согласованность идентификаторов после многоступенчатого обогащения и переименования; Pandera-схема `DocumentSchema` служит финальной проверкой. [ref: repo:src/bioetl/pipelines/document.py@test_refactoring_32]
- Отчёт валидации содержит статус стадии PubMed: если обогащение отключено или недоступно, фиксируется причина и severity. [ref: repo:src/bioetl/pipelines/document.py@test_refactoring_32]
- Перед Pandera-проверкой временные колонки (`_original_title`, `pubmed_id`) удаляются, чтобы исключить дрейф схемы. [ref: repo:src/bioetl/pipelines/document.py@test_refactoring_32]

## Target (`src/bioetl/sources/target/schema/`)

- Валидация опирается на финализованные «gold» датафреймы и подтверждает сортировку/детерминизм через `finalize_output_dataset`. [ref: repo:src/bioetl/pipelines/target.py@test_refactoring_32]
- Комбинированные QC-таблицы (components, protein_class, xref) сохраняются в `stage_context` и проверяются на наличие пропусков до экспорта. [ref: repo:src/bioetl/pipelines/target.py@test_refactoring_32]
- Любая деградация enrichment (например, отключённый IUPHAR) приводит к записи предупреждения с возможностью провала по порогу. [ref: repo:src/bioetl/pipelines/target.py@test_refactoring_32]

## Test Item (`src/bioetl/sources/testitem/schema/`)

- Перед валидацией обеспечивается детерминированный индекс по `molecule_chembl_id` и очищаются дубликаты, чтобы Pandera получала уникальные ключи. [ref: repo:src/bioetl/pipelines/testitem.py@test_refactoring_32]
- При объединении с данными API контроль типов выполняется до передачи в схему `TestItemSchema`, исключая поломку nullable-полей. [ref: repo:src/bioetl/pipelines/testitem.py@test_refactoring_32]
- Ошибки HTTP/Retry передаются в QC-метрики, что позволяет отлавливать системные деградации в CI. [ref: repo:src/bioetl/pipelines/testitem.py@test_refactoring_32]
