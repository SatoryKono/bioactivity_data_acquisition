# Протокол извлечения данных ChEMBL v2.3

**Версия:** 2.3 (октябрь 2025 г.)  
**Репозиторий:** https://github.com/SatoryKono/bioactivity_data_acquisition/tree/test_refactoring_04  
**Область применения:** извлечение, нормализация и контроль качества данных о биоактивности из базы данных ChEMBL и вторичных источников, с организацией в схему звезды для воспроизводимой аналитики и формированием детерминированных артефактов (CSV, отчёты QA, метаданные YAML).  
**Статус:** одобрен для тестовой среды (см. §6).

---

## Контроль изменений — CHEMBL-DM02

| Этап        | Владелец                    | Дата       | Подпись      | Влияние на данные                                                                 |
|-------------|-----------------------------|------------|--------------|-----------------------------------------------------------------------------------|
| Подготовлен | Хранитель документации      | 2025-10-21 | CHEMBL-DM02  | Уточнены источники данных, схемы, правила постобработки, политика кэширования и метрики QA |
| Проверен    | Руководитель QA, DataOps    | 2025-10-21 | CHEMBL-DM02  | Проверена согласованность, разрешение конфликтов источников, устойчивость пайплайна       |
| Одобрен     | Руководитель управления данными | 2025-10-21 | CHEMBL-DM02  | Одобрен для тестовой среды                                                        |

---

## 1. Введение

Протокол стандартизирует извлечение, нормализацию и контроль качества данных о биоактивности из ChEMBL и вторичных источников. Определяются пять основных таблиц (documents, targets, assays, test items, activities), организованных в схему звезды. Документ соответствует ветке `test_refactoring_04` репозитория и фиксирует требования к целостности, дедупликации и нормализации. Основной источник — ChEMBL; вторичное обогащение: PubMed, Semantic Scholar, Crossref, OpenAlex, UniProt, IUPHAR/Guide to Pharmacology, PubChem. Выходные данные публикуются в CSV, с сопроводительными отчётами о качестве и `meta.yaml` для трассируемости. Для ChEMBL и сопутствующих API используются официальные REST-эндпоинты и руководства поставщиков, что обеспечивает стабильность интеграции. :contentReference[oaicite:0]{index=0}

### 1.1. Среда выполнения

Зависимости определяются в `pyproject.toml`; фактические версии фиксируются lock-файлом и рассматриваются как единственный источник истины. Ручные перечисления версий в протоколе запрещены. Сборка валидна только при прохождении линтинга и юнит-тестов в CI. Валидация схем данных реализована на базе Pandera (DataFrameSchema: колонки, типы, nullability, ограничения), что обеспечивает единообразную проверку на уровне фреймов данных. :contentReference[oaicite:1]{index=1}

---

## 2. Схемы данных

Модель следует схеме звезды: центральная таблица фактов `activity` ссылается на измерения `documents`, `targets`, `assays`, `test items`. Внешние ключи обязаны быть ненулевыми и ссылаться на валидные первичные ключи; факты без валидных измерений исключаются. Опорные определения реализованы Pandera-моделями в `library/schemas/*` с расширенными валидаторами доменных диапазонов (например, `year` 1800–2025), whitelists для единиц (`standard_units`) и обязательной непропущенности FK. Списки столбцов ниже отражают ядро модели; доп. поля допускаются в реализациях и остаются вне спецификации для сохранения фокуса на инвариантах схем. :contentReference[oaicite:2]{index=2}

### 2.1. Documents (Документы)

| Столбец             | Тип | Ограничения            | Описание |
|---------------------|-----|------------------------|----------|
| document_chembl_id  | str | ≠ null, уникальный     | Уникальный идентификатор ChEMBL; ключ для ссылочной целостности и связывания с `activities`. |
| doi                 | str | ∪ ∅, валидный DOI      | Цифровой идентификатор объекта; валидируется по стандартному шаблону; используется для обогащения из Crossref/OpenAlex. :contentReference[oaicite:3]{index=3} |
| title               | str | ≠ null                 | Название публикации; нормализация пробелов и кодировки; используется для индексации. |
| journal             | str | ∪ ∅                    | Издание/журнал; нормализация аббревиатур для сопоставимости. |
| year                | int | ∪ ∅, 1800–2025         | Год публикации; при отсутствии восстанавливается из вторичных источников с приоритетом PubMed. :contentReference[oaicite:4]{index=4} |
| document_pubmed_id  | str | ∪ ∅                    | PMID для кросс-ссылок и обогащения библиометрией; формат целого числа. :contentReference[oaicite:5]{index=5} |

Ссылка на имплементацию: `library/schemas/document_spec.py` [F:library/schemas/document_spec.py†L13-L118].

### 2.2. Targets (Цели)

| Столбец         | Тип | Ограничения            | Описание |
|-----------------|-----|------------------------|----------|
| target_chembl_id| str | ≠ null, уникальный     | Уникальный идентификатор цели ChEMBL; связывает с `assays` и `activities`. |
| pref_name       | str | ≠ null                 | Предпочтительное название; учитываются синонимы из UniProt. :contentReference[oaicite:6]{index=6} |
| organism        | str | ∪ ∅                    | Организм; нормализация по таксономии. |
| uniprot_id      | str | ∪ ∅                    | Идентификатор UniProtKB (например, P12345); валидируется по формату. :contentReference[oaicite:7]{index=7} |
| protein_class   | str | ∪ ∅                    | Класс белка по IUPHAR; используется для категоризации. :contentReference[oaicite:8]{index=8} |

Ссылка: `library/schemas/targets.py` [F:library/schemas/targets.py†L18-L124].

### 2.3. Assays (Анализы)

| Столбец          | Тип | Ограничения          | Описание |
|------------------|-----|----------------------|----------|
| assay_chembl_id  | str | ≠ null, уникальный   | Уникальный идентификатор анализа ChEMBL; ключ для связи с `activities` и `targets`. |
| target_chembl_id | str | ≠ null, FK → targets | Проверка на наличие в `targets`; orphan-строки исключаются. |
| assay_type       | str | ≠ null               | Тип анализа (напр., Binding); нормализация по словарям ChEMBL. :contentReference[oaicite:9]{index=9} |
| bao_endpoint     | str | ∪ ∅                  | Конечная точка BAO; маппинг по локальным словарям. |
| description      | str | ∪ ∅                  | Описание протокола/условий; очистка от HTML и мусора. |
| assay_parameters | str | ∪ ∅, JSON            | JSON-словарь условий (например, `{"temperature": 37, "pH": 7.4}`), извлекаемый из ChEMBL и BAO-терминов; валидируется по внутренней схеме. :contentReference[oaicite:10]{index=10} |

Ссылка: `library/schemas/assays.py` [F:library/schemas/assays.py†L33-L59].

### 2.4. Test Items (Тестовые элементы, молекулы)

| Столбец                  | Тип   | Ограничения     | Описание |
|--------------------------|-------|-----------------|----------|
| molecule_chembl_id       | str   | ≠ null, уникальный | Идентификатор молекулы ChEMBL; PK для связи с `activities`. :contentReference[oaicite:11]{index=11} |
| canonical_smiles         | str   | ∪ ∅             | Канонический SMILES; проверка синтаксиса для совместимости с хим. инструментами. |
| molecule_type            | str   | ∪ ∅             | Тип соединения (например, small molecule) для аналитической фильтрации. |
| full_mwt                 | float | ∪ ∅, ≥0         | Молекулярная масса; уточняется из PubChem при доступности. :contentReference[oaicite:12]{index=12} |
| parent_molecule_chembl_id| str   | ∪ ∅             | Родительская молекула для группировок солей/дериватов. |

Ссылка: `library/schemas/testitems.py` [F:library/schemas/testitems.py†L12-L44].

### 2.5. Activities (Активности)

| Столбец            | Тип   | Ограничения          | Описание |
|--------------------|-------|----------------------|----------|
| activity_id        | str   | ≠ null, уникальный   | Центральный ключ фактовой таблицы; уникализирует измерения. |
| assay_chembl_id    | str   | ≠ null, FK → assays  | Ссылочная целостность к анализам. |
| molecule_chembl_id | str   | ≠ null, FK → test items | Ссылочная целостность к молекулам. |
| target_chembl_id   | str   | ≠ null, FK → targets | Ссылочная целостность к целям. |
| document_chembl_id | str   | ≠ null, FK → documents | Привязка к источнику публикации. |
| standard_value     | float | ∪ ∅, ≥0              | Нормализованное значение активности (например, IC50 в nM). |
| standard_units     | str   | ∪ ∅, whitelist       | Единицы; конвертация к nM для унификации. |
| pchembl_value      | float | ∪ ∅, вычисляемое     | −log10 активности; округление до 2 знаков. |
| standard_relation  | str   | ∪ ∅                  | Отношение к значению (“=”, “<”, “>”) для учёта цензуры. |

Ссылка: `library/schemas/activities.py` [F:library/schemas/activities.py†L31-L83].

---

## 3. Источники данных

Извлечение опирается на ChEMBL (основной поставщик фактов) и вторичные источники (для обогащения documents, targets, test items). Используются официальные REST-интерфейсы; набор полей минимизируется за счёт параметризованных запросов и пагинации.

| Источник | Эндпоинт | Извлекаемые поля (минимум) | Затрагиваемые таблицы |
|---|---|---|---|
| ChEMBL REST API | `https://www.ebi.ac.uk/chembl/api/data` | document_chembl_id, doi, title, journal, year, document_pubmed_id; target_chembl_id, pref_name, organism; assay_chembl_id, assay_type, bao_endpoint, description; molecule_chembl_id, canonical_smiles, molecule_type, full_mwt, parent_molecule_chembl_id; activity_id, standard_value, standard_units, pchembl_value, standard_relation | Documents, Targets, Assays, Test Items, Activities. :contentReference[oaicite:13]{index=13} |
| PubMed (E-utilities) | `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/` | document_pubmed_id, title, journal, year | Documents. :contentReference[oaicite:14]{index=14} |
| Semantic Scholar Graph API | `https://api.semanticscholar.org/graph/v1/paper` | title, journal, year | Documents. |
| Crossref REST | `https://api.crossref.org/works` | doi, title, journal, year | Documents. :contentReference[oaicite:15]{index=15} |
| OpenAlex | `https://api.openalex.org/works` | doi, title, journal, year | Documents. :contentReference[oaicite:16]{index=16} |
| UniProt | `https://rest.uniprot.org/uniprotkb` | uniprot_id, organism; доп. аннотации | Targets. :contentReference[oaicite:17]{index=17} |
| IUPHAR/GtoP | `https://www.guidetopharmacology.org/webServices` | protein_class; кластеры таргетов | Targets. :contentReference[oaicite:18]{index=18} |
| PubChem PUG-REST | `https://pubchem.ncbi.nlm.nih.gov/rest/pug` | canonical_smiles, full_mwt, parent | Test Items. :contentReference[oaicite:19]{index=19} |

**Примечания.** ChEMBL — основной источник ключей и связей; вторичные источники дополняют документы (библиометрия), цели (белковые аннотации) и тест-элементы (структурные свойства). Локальные словари (`config/dictionary/*`) обеспечивают детерминированные сопоставления BAO и классификаций. Кросс-маппинг ChEMBL↔UniProt выполняется стандартными REST-запросами с последующей верификацией формата и дедупликацией соответствий. Последовательность обогащения документов: PubMed → Crossref → OpenAlex → Semantic Scholar; при конфликтах применяется приоритет источника и полноты записи. :contentReference[oaicite:20]{index=20}

### 3.1. Стратегия кэширования

Локальное кэширование с TTL на источник: Crossref/OpenAlex — 7 дней; ChEMBL — 1 день; UniProt — 14 дней; PubChem — 7 дней. TTL управляет инвалидированием кэша для баланса свежести и стоимости запросов; значения TTL — внутренняя политика проекта и не являются ограничениями провайдеров.

---

## 4. Потоки постобработки

Сырые слои преобразуются в нормализованные и финальные выводы. Для каждого набора таблиц в `library/postprocessing/*` определены шаги: нормализация (очистка, типы), обогащение (внешние источники), валидация (ограничения схем), дедупликация (по `hash_row`), расчёт производных полей (например, pChEMBL). Логирование шагов обеспечивает трассируемость; сырые промежуточные артефакты сохраняются для аудита.

### 4.1. Documents (Документы)

| Шаг | Вход (raw) | Преобразования | Выход (normalized) |
|---|---|---|---|
| normalize_document_fields | document_chembl_id, doi, title, journal, year, document_pubmed_id | Тримминг строк, кодировка, приведение `year` к int; проверка DOI по регэкспу; `document_chembl_id` обязательный; фиксация отказов с причинами. | document_chembl_id, doi, title, journal, year, document_pubmed_id |
| enrich_document_publication_year | year | Восстановление `year` из вторичных источников по PMID/DOI с приоритетом PubMed; диапазон 1800–2025; отбраковка аномалий. :contentReference[oaicite:21]{index=21} | year |
| finalize_document_records | Все | `hash_row` = SHA256(поля в детермин. порядке); дедуп по `hash_row`; фильтрация по стоп-словарю (предаторские журналы); отчёт о дубликатах/изменениях. | Все + `hash_row` |

См. имплементацию: `library/postprocessing/documents/steps.py` [F:library/postprocessing/documents/steps.py†L1-L82].

### 4.2. Targets (Цели)

| Шаг | Вход (raw) | Преобразования | Выход (normalized) |
|---|---|---|---|
| normalize_target_fields | target_chembl_id, pref_name, organism, uniprot_id, protein_class | Тримминг, нормализация регистра; маппинг организмов к таксономии; валидация формата UniProt; PK ненулевой. :contentReference[oaicite:22]{index=22} | target_chembl_id, pref_name, organism, uniprot_id, protein_class |
| enrich_target_synonyms | uniprot_id, protein_class | Обогащение синонимами/классами из UniProt и IUPHAR; ошибки 4xx логируются; при конфликтах приоритет UniProt. :contentReference[oaicite:23]{index=23} | uniprot_id, protein_class |
| finalize_target_records | Все | `hash_row`; дедуп; сохранение совместимых legacy-полей; отчёт о покрытии обогащения. | Все + `hash_row` |

См.: `library/postprocessing/targets/steps.py` [F:library/postprocessing/targets/steps.py†L24-L76].

### 4.3. Assays (Анализы)

| Шаг | Вход (raw) | Преобразования | Выход (normalized) |
|---|---|---|---|
| normalize_assay_metadata | assay_chembl_id, target_chembl_id, assay_type, bao_endpoint, description | Нормализация `assay_type` по словарям ChEMBL; проверка FK в `targets`; очистка описаний. :contentReference[oaicite:24]{index=24} | assay_chembl_id, target_chembl_id, assay_type, bao_endpoint, description |
| enrich_assay_flags | bao_endpoint | Маппинг BAO на стандартные термины; добавление флагов; извлечение `assay_parameters` из ChEMBL/BAO; валидация JSON-схемой. :contentReference[oaicite:25]{index=25} | bao_endpoint |
| finalize_assay_records | Все | `hash_row`; дедуп по `hash_row` и business-key; фильтра по BAO-coverage; аудит изменений. | Все + `hash_row` |

См.: `library/postprocessing/assays/steps.py` [F:library/postprocessing/assays/steps.py†L20-L73].

### 4.4. Test Items (Тестовые элементы)

| Шаг | Вход (raw) | Преобразования | Выход (normalized) |
|---|---|---|---|
| prepare_parent_enrichment | molecule_chembl_id, canonical_smiles, full_mwt | Валидация/канонизация SMILES; подготовка запросов SMILES → CID (PUG-REST). | molecule_chembl_id, canonical_smiles, full_mwt |
| run_parent_enrichment | molecule_chembl_id, canonical_smiles, full_mwt | Получение `full_mwt`, `parent` через PUG-REST; fallback на ChEMBL; логирование дельт. :contentReference[oaicite:26]{index=26} | canonical_smiles, full_mwt, parent_molecule_chembl_id |
| finalize_output | Все | `hash_row` по структурным полям; дедуп; проверка `mwt ≥ 0`; отчёт о покрытии (>95%). | Все + `hash_row` |

См.: `library/pipelines/testitem/cli.py` [F:library/pipelines/testitem/cli.py†L864-L1186].

### 4.5. Activities (Активности)

| Шаг | Вход (raw) | Преобразования | Выход (final) |
|---|---|---|---|
| normalize_activity_records | standard_value, standard_units | Приведение единиц к nM (uM ×1000; mM ×1e6); учёт цензуры через `standard_relation` и интервальные поля; whitelist units. | standard_value, standard_units, standard_relation |
| enrich_activity_quality | activity_id, standard_value | Флаги качества (exact vs censored), сводная статистика по группам, контекст из `assays`. | quality_flag |
| finalize_activity_records | Все | `pchembl_value = −log10(value[nM] × 1e−9)`; `hash_row`; удаление временных колонок; дедуп; проверка всех FK. | Все + `hash_row`, `pchembl_value` |

См.: `library/cli/entrypoints/activity.py` [F:library/cli/entrypoints/activity.py†L1239-L1360].

### 4.6. Финализация вывода

Экспорт детерминирован: фиксированный порядок колонок (как в схемах), сортировка по первичным ключам, стабильные форматы чисел и политика NA. Повторные прогоны при неизменных входах дают идентичные CSV.

---

## 5. Обеспечение качества и валидация

QA гарантирует целостность и согласованность данных.

- **Валидация Pandera** для финальных таблиц: обязательная проверка колонок, типов, nullability и доменных ограничений; ошибки эмитятся в `*_failure_cases.csv`. :contentReference[oaicite:27]{index=27}  
- **Метрики качества**: формируются `library/qa/table_quality.py` и включают объём строк, пропуски, дубликаты, покрытия нормализации; результаты в `<stem>_quality_report_table.csv` и `<stem>.postprocess.report.json`.  
- **Дедупликация**: по `hash_row` (SHA256) и при необходимости `hash_business_key`.  
- **Отчётность**: JSON/CSV-отчёты пригодны для машинной агрегации в CI.

### 5.1. Контрольный список QA по таблицам

| Метрика                                   | Documents | Targets | Assays | Test Items | Activities |
|-------------------------------------------|-----------|---------|--------|------------|------------|
| Валидные строки (после дедупа)            | >0        | >0      | >0     | >0         | >0         |
| Доля пропусков в ключевых полях           | <10%      | <15%    | <20%   | <5%        | <10%       |
| Дубликаты (`hash_row`)                    | 0%        | 0%      | 0%     | 0%         | 0%         |
| Покрытие обогащений/нормализаций          | >90%      | >85%    | >80%   | >95%       | >90%       |
| Доля цензурированных значений             | —         | —       | —      | —          | отчётно    |

---

## 6. Локализация и выходные файлы

Поддерживаются английская (`docs/en/PROTOCOL_EN.md`) и русская (`docs/ru/PROTOCOL_RU.md`) версии документа. Генерация DOCX выполняется из актуального Markdown; бинарники не коммитятся. Русскоязычные артефакты валидируются на UTF-8.

---

## 7. Журнал изменений

| Версия | Дата       | Автор                  | Ключевые обновления |
|--------|------------|------------------------|---------------------|
| 2.3    | 2025-10-21 | Хранитель документации | Добавлены среда выполнения, TTL-кэширование, расширенные схемы/валидаторы, разделение raw/normalized, правила единиц/pChEMBL, поле `assay_parameters`, разрешение конфликтов источников, контрольный список QA, уточнение FK. |
| 2.2    | 2025-11-01 | Хранитель документации | Усилён фокус на схемах данных, источниках и постобработке; добавлена локализация. |
| 2.1    | 2025-10-15 | Совет по релизу DocsOps | Восстановлена одобренная базовая линия; согласованы метаданные релиза с октябрём 2025 г. |

---

## Приложение A. Справочные ссылки на официальные API/документацию

- **ChEMBL Data Web Services:** база URL, пагинация, ресурсы. :contentReference[oaicite:28]{index=28}  
- **NCBI E-utilities (Entrez/PubMed):** обзор, синтаксис и примеры. :contentReference[oaicite:29]{index=29}  
- **Crossref REST API:** works-эндпоинт, советы по использованию. :contentReference[oaicite:30]{index=30}  
- **OpenAlex API:** сущность Works, лимиты, обзор. :contentReference[oaicite:31]{index=31}  
- **UniProt REST API:** UniProtKB и програмный доступ. :contentReference[oaicite:32]{index=32}  
- **IUPHAR/Guide to Pharmacology:** веб-сервисы, назначение. :contentReference[oaicite:33]{index=33}  
- **PubChem PUG-REST:** документация и туториал. :contentReference[oaicite:34]{index=34}  
- **Pandera:** DataFrameSchema и справочник API. :contentReference[oaicite:35]{index=35}
