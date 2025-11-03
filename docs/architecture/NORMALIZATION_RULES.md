# NORMALIZATION_RULES.md

Нормативный реестр нормализации описывает единые правила для каталога `src/bioetl/sources/<source>/normalizer/` и смежных слоёв. Структура слоёв фиксирована в [MODULE_RULES.md](../../refactoring/MODULE_RULES.md) и служит входом для ревью архитектуры.

## Общие инварианты (MUST)

- Нормализация выполняется через `bioetl.normalizers.registry.normalize`, чтобы гарантировать единый контроль типов и обработку ошибок. [ref: repo:src/bioetl/normalizers/registry.py@test_refactoring_32]
- Каждый нормализатор обязан возвращать значения, совместимые с Pandera-схемой соответствующего источника до запуска валидации. [ref: repo:src/bioetl/schemas/base.py@test_refactoring_32]
- Временные вычисления (хеши, ключи объединения) выполняются в нормализаторе, чтобы запись и валидация работали с уже приведёнными данными. [ref: repo:src/bioetl/pipelines/activity.py@test_refactoring_32]

## Activity (`src/bioetl/sources/activity/normalizer/`)

- `compound_key` формируется как конкатенация `molecule_chembl_id`, `standard_type`, `target_chembl_id`, что обеспечивает стабильный бизнес-ключ для fallback и дедупликации. [ref: repo:src/bioetl/pipelines/activity.py@test_refactoring_32]
- Флаги цитирования (`is_citation`, `exact_data_citation`, `rounded_data_citation`, `high_citation_rate`) извлекаются из свойств ответа и приводятся к булевому типу через реестр нормализации. [ref: repo:src/bioetl/pipelines/activity.py@test_refactoring_32]
- Поля с числовыми ограничениями (`standard_value`, `published_value`) дополнительно проходят `registry.normalize("numeric", …)` и контроль неотрицательности. [ref: repo:src/bioetl/pipelines/activity.py@test_refactoring_32]

## Assay (`src/bioetl/sources/assay/normalizer/`)

- Строковые поля (`assay_description`, `assay_type`) нормализуются как строковые значения, удаляя артефакты формата и регистра. [ref: repo:src/bioetl/pipelines/assay.py@test_refactoring_32]
- Таблицы обогащения по таргетам и классам приводятся к whitelist-набору столбцов и синхронизируются с `TARGET_ENRICHMENT_WHITELIST` и `ASSAY_CLASS_ENRICHMENT_WHITELIST`. [ref: repo:src/bioetl/pipelines/assay.py@test_refactoring_32]
- Nullable-интегральные поля (`confidence_score`, `assay_class_id` и др.) приводятся через `coerce_nullable_int` до попадания в Pandera-валидацию. [ref: repo:src/bioetl/pipelines/assay.py@test_refactoring_32]

## Document (`src/bioetl/sources/document/normalizer/`)

- Идентификаторы (`document_chembl_id`, `doi`, `pmid`) приводятся через `registry.normalize("identifier", …)` перед обогащением и переименованием полей. [ref: repo:src/bioetl/pipelines/document.py@test_refactoring_32]
- Булевые признаки (`original_experimental_document`, `referenses_on_previous_experiments`) строятся через специализированные coercion-хелперы и сохраняются в нормализованном датафрейме. [ref: repo:src/bioetl/pipelines/document.py@test_refactoring_32]
- Переименование колонок выполняется по стабильной карте `field_mapping`, чтобы многослойное слияние данных (ChEMBL, PubMed, Semantic Scholar) соблюдало контракт схемы. [ref: repo:src/bioetl/pipelines/document.py@test_refactoring_32]

## Target (`src/bioetl/sources/target/normalizer/`)

- Идентификаторы (`target_chembl_id`, `hgnc_id`, `uniprot_accession`) нормализуются через `registry.normalize("identifier", …)` для консистентной стыковки с UniProt и IUPHAR. [ref: repo:src/bioetl/pipelines/target.py@test_refactoring_32]
- Названия таргетов (`pref_name`) приводятся к очищенному строковому виду для стабильного экспорта и QC. [ref: repo:src/bioetl/pipelines/target.py@test_refactoring_32]
- Обогащения (`uniprot`, `iuphar`) выполняются до формирования «gold» наборов, после чего итоговые фреймы проходят финализацию с сортировкой по бизнес-ключу. [ref: repo:src/bioetl/pipelines/target.py@test_refactoring_32]

## Test Item (`src/bioetl/sources/testitem/normalizer/`)

- Ключевые идентификаторы (`molecule_chembl_id`, `parent_chembl_id`) нормализуются и служат индексом при наложении данных API на входной CSV. [ref: repo:src/bioetl/pipelines/testitem.py@test_refactoring_32]
- Дубликаты входных молекул устраняются с логированием, чтобы нормализованный набор оставался детерминированным. [ref: repo:src/bioetl/pipelines/testitem.py@test_refactoring_32]
- Перекрывающиеся значения из API переопределяют входной CSV только при наличии нормализованных данных, что сохраняет воспроизводимость pipeline. [ref: repo:src/bioetl/pipelines/testitem.py@test_refactoring_32]
