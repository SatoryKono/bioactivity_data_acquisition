# Матрица компетенций и ответственности ChEMBL-пайплайнов

Документ описывает обязанности, артефакты и компетенции для всех реализованных пайплайнов, зарегистрированных в `PIPELINE_REGISTRY`. Все пайплайны наследуют `UnifiedPipelineBase`, поэтому автоматически получают единый жизненный цикл `extract → transform → validate → write`, структурированные логи, handshake с ChEMBL и детерминированную запись артефактов (см. `src/bioetl/pipelines/unified_base.py`).

## 1. Сводка

- **5 активных пайплайнов** (`activity_chembl`, `assay_chembl`, `document_chembl`, `target_chembl`, `testitem_chembl`) зарегистрированы в CLI и используют ChEMBL API как единый источник данных.
- Домены покрывают ключевые сущности экосистемы ChEMBL: активности, асайи, документы, мишени и молекулы/test items. Дополнительные кандидаты (`pubchem`, `uniprot`, и т. д.) присутствуют в реестре, но пока не реализованы.
- Каждый пайплайн владеет собственным набором артефактов (dataset + QC-метаданные) и обеспечивает связанные инварианты (идентификаторы, словари, nested-структуры). Границы ответственности проходят по линиям ETL: сбор из ChEMBL, нормализация, валидированный записью выход.
- Основной стек компетенций: Python/Pandas, ChEMBL API и схемы, механизмы оркестрации CLI, контроль качества/детерминизм, доменное знание конкретной сущности (activity/assay/... ).

## 2. Общая матрица пайплайнов

| Пайплайн (ID) | Класс / модуль | Основной домен | Источники данных | Выходные артефакты | Основные обязанности | Ключевые компетенции |
|---------------|----------------|----------------|------------------|--------------------|----------------------|----------------------|
| `activity_chembl` | `ChemblActivityPipeline` (`src/bioetl/pipelines/chembl/activity/run.py`) | Activity records + enrichments (assay/molecule/data_validity) | ChEMBL `/activity` + вспомогательные энричеры из `chembl.activity.enrich` | Паркет/CSV, партиционированный по `assay_chembl_id`, манифест, QC/корреляции | Делегированный `extract_by_ids`, нормализация измерений, валидация FK и словарей, каскадное обогащение compound/assay/molecule/data_validity | Python/Pandas, ChEMBL activity schema, ChEMBL enrichment API, CLI/детерминизм |
| `assay_chembl` | `ChemblAssayPipeline` (`src/bioetl/pipelines/chembl/assay/run.py`) | Ассайи, их классификации и TRUV-параметры | ChEMBL `/assay` + enrichment `/assay_class_map`, `/assay_parameters` | Детерминированный датасет, сортировка по `assay_chembl_id` | Handshake c release, нормализация идентификаторов, сериализация nested массивов, валидация TRUV-параметров и BAO ID | Pandas/nested-структуры, ChEMBL assay schema, BAO/TRUV домен, CLI |
| `document_chembl` | `ChemblDocumentPipeline` (`src/bioetl/pipelines/chembl/document/run.py`) | Научные документы | ChEMBL `/document` | Партиционирование по `year`, хэш-бизнес-ключ | Извлечение + нормализация DOI/PMID, дедупликация и подсчёт авторов | Pandas/текстовые нормализации, ChEMBL document API, QC/дедуп, CLI |
| `target_chembl` | `ChemblTargetPipeline` (`src/bioetl/pipelines/chembl/target/run.py`) | Белковые мишени и компоненты | ChEMBL `/target` + `/target_component`, protein-class endpoints | Датасет по `target_chembl_id`, сериализованные массивы | Нормализация идентификаторов, кеширование nested массивов, сериализация компонентов, обогащение protein class и компонентов | Pandas + JSON serialization, ChEMBL target schema, protein class домен |
| `testitem_chembl` | `TestItemChemblPipeline` (`src/bioetl/pipelines/chembl/testitem/run.py`) | Molecule/test item records | ChEMBL `/molecule` + `/status` | Датасет по `molecule_chembl_id`, включает ChEMBL DB/API версии | Handshake со `/status`, flatten молекулярных структур/свойств, сериализация массивов, добавление версионных метаданных | Pandas flattening, хим. домен (SMILES/InChI/ATC), HTTP handshake, CLI |

## 3. Детализированные матрицы по пайплайнам

### 3.1 `activity_chembl` — ChemblActivityPipeline

**Зона ответственности.** Класс извлекает активности по батчам идентификаторов, нормализует измерения, проверяет внешние ключи и выполняет многошаговое обогащение (compound record, assay, molecule, data_validity). Пайплайн также следит за корректностью переданных ID, формирует отчёты и пишет детерминированные артефакты (`configs/pipelines/activity/activity_chembl.yaml`).

**Компетенции.**

| Компетенция | Причина | Тип ответственности | Уровень |
|-------------|---------|---------------------|---------|
| Pandas и нормализация измерений | `domain_enrich` строится на последовательности нормализаций измерений, nested структур и добавления row metadata (`activity/run.py`) | Разработка | Продвинутый |
| Управление enrichment-сценариями | Пайплайн регистрирует 4 сценария (`compound_record`, `assay`, `molecule`, `data_validity`) и запускает их детерминированно (тот же модуль) | Разработка | Продвинутый |
| Делегированное извлечение по ID | `id_extraction_*` фабрики нормализуют ID, агрегируют статистику, логируют ошибки и fallback (`activity/run.py`) | Эксплуатация | Продвинутый |
| Знание схемы ChEMBL Activity | `configs/pipelines/activity/activity_chembl.yaml` перечисляет десятки полей и бизнес-ключей; без понимания домена невозможно обновить список или проверки | Дом. валидация | Продвинутый |
| CLI/детерминизм и QC | Конфиг задаёт партиционирование, сортировку, бизнес-ключ, а базовый класс обеспечивает связку с CLI | Эксплуатация | Базовый |

**Границы ответственности.**

- **Входит:** батчевый `extract`, нормализация единиц, валидация ссылок на другие сущности, enrichment и QC отчёты.
- **Не входит:** поддержка источников вне ChEMBL, агрегации/аналитика, управление upstream справочниками (осуществляется клиентами/вокабулярами).

**Входы/выходы.**

- Вход: ChEMBL `/activity` API с указанием полей (select_fields, batch_size=20).
- Доп. входы: enrichment endpoints (`chembl.activity.enrich` конфиг), список activity_id (CLI `--input-file`).
- Выход: детерминированный датасет, партиционированный по `assay_chembl_id`, вместе с manifest/QC и дополнительными enrichment артефактами.

### 3.2 `assay_chembl` — ChemblAssayPipeline

**Зона ответственности.** Обеспечивает handshake с ChEMBL release, извлекает ассайи, нормализует идентификаторы, сериализует nested массивы (classifications, parameters), валидирует BAO ID и TRUV-инварианты, а также логирует расхождения select_fields.

**Компетенции.**

| Компетенция | Причина | Тип | Уровень |
|-------------|---------|-----|---------|
| Handshake и release capture | `descriptor_spec` вызывает `perform_handshake` и логирует release | Эксплуатация | Базовый |
| Работа с nested структурами/серилизацией | `domain_enrich` + `nested_column_specs` сериализуют arrays/JSON | Разработка | Продвинутый |
| TRUV/BAO домен | `_normalize_nested_structures` валидирует TRUV и извлекает BAO идентификаторы | Дом. валидация | Продвинутый |
| Управление select_fields | Пайплайн сравнивает ожидаемые и полученные поля, логирует пропуски | Эксплуатация | Базовый |

**Границы ответственности.**

- **Входит:** handshake, нормализация идентификаторов, enrichment классификаций и параметров, сериализация nested данных.
- **Не входит:** агрегации по результатам ассая, управление external vocab (только проверка).

**Входы/выходы.**

- Вход: ChEMBL `/assay` со списком полей; enrichment конфигурация для classifications/parameters.
- Выход: датасет с сортировкой/хешами, поддерживающий массивы в header-row формате.

### 3.3 `document_chembl` — ChemblDocumentPipeline

**Зона ответственности.** Извлекает документы, нормализует DOI/PMID, рассчитывает авторские метаданные, устраняет дубликаты, добавляет системные поля (`source`). Партиционирует данные по `year`.

**Компетенции.**

| Компетенция | Причина | Тип | Уровень |
|-------------|---------|-----|---------|
| Текстовые нормализации DOI/авторов | `_normalize_doi`, `_normalize_authors` и подсчёт `authors_count` | Разработка | Продвинутый |
| Валидация и дедуп | `_check_document_id_uniqueness` и `_deduplicate_documents` | Эксплуатация | Базовый |
| Доменные знания публикаций | Схема включает `journal`, `pubmed_id`, `year` — требуются бизнес-правила качества | Дом. валидация | Базовый |

**Границы ответственности.**

- **Входит:** извлечение/нормализация библиографических данных, добавление источника и хэшей.
- **Не входит:** полнотекстовый парсинг, обогащения внешними библио-базами (только ChEMBL).

**Входы/выходы.**

- Вход: ChEMBL `/document`.
- Выход: датасет с партиционированием по `year`, хэшами бизнес-ключей.

### 3.4 `target_chembl` — ChemblTargetPipeline

**Зона ответственности.** Собирает мишени, сериализует nested массивы (`cross_references`, `target_components`), кэширует исходные массивы, обогащает сведениями о компонентах и protein class, нормализует идентификаторы и сортирует выход.

**Компетенции.**

| Компетенция | Причина | Тип | Уровень |
|-------------|---------|-----|---------|
| Работа с массивами/сериализацией | `_array_source_cache` + `serialize_target_arrays` | Разработка | Продвинутый |
| Обогащение компонентами и protein class | `_enrich_target_components`, `_enrich_protein_classifications` | Дом. валидация | Продвинутый |
| Контроль идентификаторов | `identifier_rules` и `postprocess_identifier_columns` логируют нарушения | Эксплуатация | Базовый |
| CLI/драйверы | Настройка batch size и release capture в `descriptor_spec` | Эксплуатация | Базовый |

**Границы ответственности.**

- **Входит:** извлечение/нормализация мишеней, обогащение компонентами и protein class, сериализация массивов.
- **Не входит:** обработка downstream моделей/аннотаций вне ChEMBL.

**Входы/выходы.**

- Вход: ChEMBL `/target` + `/target_component` и классификации.
- Выход: датасет с сериализованными массивами, бизнес-ключ `target_chembl_id`.

### 3.5 `testitem_chembl` — TestItemChemblPipeline

**Зона ответственности.** Отвечает за извлечение молекул, handshake со статусом ChEMBL (release + API version), flatten nested объектов (molecule_structures/properties/hierarchy), сериализацию массивов (ATC, cross_references, synonyms) и добавление версионных полей.

**Компетенции.**

| Компетенция | Причина | Тип | Уровень |
|-------------|---------|-----|---------|
| HTTP handshake / release capture | `_fetch_chembl_release` вызывает `/status`, логирует версии и пишет метаданные | Эксплуатация | Базовый |
| Flatten/serialize nested хим. структур | `transform` вызывает `transform_testitem`, нормализует числовые поля, сериализует массивы | Разработка | Продвинутый |
| Доменные знания хим. идентификаторов | `identifier_rules` охватывают `molecule_chembl_id`, `standard_inchi_key` | Дом. валидация | Продвинутый |
| Контроль версионности артефактов | `augment_metadata` добавляет `chembl_db_version`/`api_version` | Эксплуатация | Базовый |

**Границы ответственности.**

- **Входит:** получение молекулярных данных, flatten структур и свойств, добавление версий ChEMBL, дедупликация, сортировка.
- **Не входит:** внешние источники (PubChem и др.), расчёт physicochemical свойств (берутся из API).

**Входы/выходы.**

- Вход: ChEMBL `/molecule` (включая nested объекты) + `/status` для версий.
- Выход: датасет по `molecule_chembl_id` с дополнительными версиями и сериализованными массивами.

## 4. Связи и владение артефактами

| Артефакт | Владелец | Зависимые пайплайны | Комментарий |
|----------|----------|----------------------|-------------|
| `activity_chembl` dataset (partition by `assay_chembl_id`) | ChemblActivityPipeline | Нет прямых upstream, но данные содержат ссылки на assay/target/document/testitem | FK проверки обеспечивают согласованность с остальными датасетами |
| `assay_chembl` dataset | ChemblAssayPipeline | ChemblActivityPipeline (использует `assay_chembl_id` / `assay_group` для enrich и валидации) | Колонки `target_chembl_id`/`document_chembl_id` связывают с другими наборами |
| `document_chembl` dataset | ChemblDocumentPipeline | ChemblActivityPipeline, ChemblAssayPipeline (используют `document_chembl_id`) | Партиционирование по `year` облегчает джойны по времени |
| `target_chembl` dataset | ChemblTargetPipeline | ChemblActivityPipeline, ChemblAssayPipeline (колонки `target_chembl_id`) | Обогащения protein-class используются активностями/ассаями при анализе |
| `testitem_chembl` dataset | TestItemChemblPipeline | ChemblActivityPipeline (колонки `testitem_chembl_id` и `molecule_chembl_id`) | Также даёт словарь для downstream витрин (test items) |

## 5. Кандидаты и открытые вопросы

- CLI-реестр содержит шесть не реализованных пайплайнов (`pubchem`, `uniprot`, `gtp_iuphar`, `openalex`, `crossref`, `pubmed`, `semantic_scholar`). Их компетенции не определены — требуется отдельное проектирование перед разработкой.
- Проверить покрытие словарей/вокабуляров для activity pipeline (использует `required_vocab_ids`), чтобы согласовать зоны ответственности между пайплайнами и глобальными сервисами словарей.
