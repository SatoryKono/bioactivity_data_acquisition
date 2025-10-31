Требования к перечню источников данных

0. Нормативная база

Ключевые слова требований интерпретируются по RFC 2119/BCP 14: MUST, MUST NOT, SHOULD, MAY. 
datatracker.ietf.org
+1

1. Область действия

Документ устанавливает обязательный состав и формат «перечня источников данных» для ветки test_refactoring_11 репозитория SatoryKono/bioactivity_data_acquisition. Входит: инвентаризация источников, их публичные интерфейсы, извлекаемые сущности и поля, правила нормализации и валидации, политика merge/join, артефакты и критерии приемки. Не входит: оркестрация пайплайнов, производительность, прикладная аналитика.

**Термины:**
- **Основной источник** — источник, из которого формируется базовый набор полей сущности.
- **Дополнительный источник (обогащение)** — источник, используемый для дополнения или уточнения отдельных полей при наличии соответствия по идентификаторам.
- **Бизнес-ключ** — минимальный набор полей, однозначно идентифицирующий запись на уровне предметной области.
- **Канонизация идентификаторов** — приведение идентификаторов к нормативной форме (регистр, префиксы, формат).
- **Контролируемые словари/онтологии** — фиксированные перечни допустимых значений или ссылочные онтологии.

**Контекст проекта:** ETL ориентирован на извлечение биоактивностных данных из ChEMBL и внешних источников с последующей нормализацией и проверками схем.

## 1.1 Матрица источников по сущностям

| Сущность | Основной источник | Доп. источники (обогащение) | Обязательные поля (уровень данных) | Идентификаторы/ключи (формат) | Онтологии/словари | Политика слияния (кратко) |
| --- | --- | --- | --- | --- | --- | --- |
| `documents` | ChEMBL Documents API | PubMed, Crossref, OpenAlex, Semantic Scholar | `document_chembl_id`, `title`, `year` или `date`, хотя бы один из `doi`/`pmid` | БК: `document_chembl_id`; внешние: `doi` (канонический `10.*`), `pmid` (целое) | — | Базовые поля из ChEMBL; `doi`/`title`/`venue`/`date` при конфликте: Crossref > PubMed > OpenAlex > ChEMBL; авторы: PubMed > Crossref; ссылки агрегируются с дедупликацией |
| `targets` | ChEMBL Target API | UniProt, IUPHAR (Guide to Pharmacology) | `target_chembl_id`, `organism`, `target_type`; при наличии — `uniprot_accession`, `gene_symbol` | БК: `target_chembl_id`; внешние: `uniprot_accession` | Таксономия NCBI (для `organism`), номенклатура UniProt (аккессия, ген) | Маркерные поля (`name`, `gene_symbol`, `organism`) при конфликте: UniProt > ChEMBL; классификация мишени: IUPHAR > ChEMBL |
| `assays` | ChEMBL Assay API | BAO (онтология биологических ассайев) | `assay_chembl_id`, `assay_type`, `assay_category`, `target_chembl_id?`, `document_chembl_id?` | БК: `assay_chembl_id` | BAO для `assay_type`/`category`/`format` | Тип/категория нормализуются к BAO; при расхождении: BAO-карта > ChEMBL raw |
| `testitems` | ChEMBL Molecule API | PubChem (идентификаторы, вещества/соли, синонимы) | `molecule_chembl_id`; при наличии — `preferred_name`, `synonyms[]`, `parent`, `salt_id` | БК: `molecule_chembl_id`; внешние: PubChem CID | — | Имена/синонимы: PubChem > ChEMBL; соль/`parent` согласуются по карте соответствий; конфликты помечаются |
| `activities` | ChEMBL Activity API | — (нормативы единиц и метрик из схем) | `activity_id?`, `assay_chembl_id`, `molecule_chembl_id`, `standard_type`, `standard_relation`, `standard_value`, `standard_units`; опц.: `pchembl_value` | БК: комбинация (`assay_chembl_id`, `molecule_chembl_id`, `standard_type`, `relation`, `timepoint?`) | Словарь допустимых метрик (IC50, EC50, Ki и др.); словарь единиц (нМ, мкМ и т.п.) | Единицы гармонизируются к целевым (см. DATA-07); при конфликтных измерениях выбирается запись с валидной единицей, меньшей степенью преобразования, лучшим признаком качества |

2. Обязательные артефакты (MUST)

docs/requirements/SOURCES_AND_INTERFACES.md

docs/requirements/NORMALIZATION_RULES.md

docs/requirements/VALIDATION_RULES.md

docs/requirements/REFACTOR_PLAN.md

Все артефакты коммитятся в одном PR при появлении/изменении источника. MUST быть детерминированная сортировка таблиц и стабильный column_order.

3. Инвентаризация (MUST)

Сформировать таблицу по каталогам src/, configs/, docs/requirements/, tests/ со столбцами:
path | module | size_kb | loc | mtime | top_symbols | imports_top | docstring_first_line.
Каждый путь указывать ссылкой вида [ref: repo:<path>@test_refactoring_11]. Результат вкладывается в SOURCES_AND_INTERFACES.md и при изменениях зеркалируется в REFACTOR_PLAN.md.

4. Стандарт «карточки источника» (MUST)

Для каждого внешнего источника/энрихера в SOURCES_AND_INTERFACES.md фиксируется единая карточка:

source и соответствующий каталог в репозитории: [ref: repo:src/bioetl/sources/<name>@test_refactoring_11]

public_api: имя публичного класса/фасада и файл pipeline.py

config_keys: список ключей конфигурации

config_path: `src/bioetl/configs/pipelines/<source>.yaml` (MUST). Допускается подключать include-модули вроде
`includes/chembl_source.yaml` для общих параметров; итоговая конфигурация проходит автоматическую валидацию `PipelineConfig`.

entities: извлекаемые сущности и обязательные поля

merge_policy: ключи join/merge и приоритеты конфликтов

tests_required: unit (client/parser/normalizer/schema), e2e (golden)

risks: известные неоднозначности и пределы API

5. Нормализация (MUST)

Общие правила для всех источников, фиксируются в NORMALIZATION_RULES.md:

Идентификаторы:

DOI в lowercase, без URL-префиксов, в поле doi_norm (MUST).

CHEMBL\d+ для ChEMBL; cid/sid числовые для PubChem; UniProt AC с отделением версии (MUST).

Авторы: массив структур с family, given, sequence:int, orcid в lowercase без URL; дедуп внутри одного документа (MUST).

Журналы: journal_title в NFC; ISSN проверять по \d{4}-\d{3}[\dxX] (MUST).

Даты: YYYY-MM-DD по UTC; при меньшей точности — заполнитель и date_precision (MUST).

Единицы активностей (ChEMBL): таблица соответствий типов/единиц и явные коэффициенты преобразования (например, µM→nM ×1000). Неизвестные единицы порождают UnitNormalizationError (MUST).

NA/типы: строгая типизация, допускаемые NaN перечислены явно (MUST).

Дедуп:

Документы: ключ (doi_norm | pmid | openalex_id | paperId) с приоритетом источников.

Активности: ключ (assay_id, molecule_id, standard_type, relation, value, units) + хеш строки (MUST).

6. Валидация (MUST)

Описывается в VALIDATION_RULES.md:

Pandera-схемы и реестр версий для каждой сущности; инварианты отдельно.

Golden-наборы CSV/Parquet на источник с checksum и ожидаемыми метриками.

FK-проверки: documents ↔ assays ↔ activities, molecules ↔ activities, targets ↔ activities.

Политика ошибок: критические — исключения; мягкие — QC-отчет.

7. Merge/Join (MUST)

Для всех объединений задокументированы ключи слияния и стратегия разрешения конфликтов (prefer_source, prefer_fresh, concat_unique, score_based); слияния выполняются после валидации обеих сторон.

**Приоритезация и разрешение конфликтов:**

**Документы (`documents`):**
- `doi`, `title`, `container_title`/`journal`, `published_(print|online)_date`: Crossref > PubMed > OpenAlex > ChEMBL.
- `authors`: PubMed > Crossref; дедупликация по (`surname`, `initials`); порядок как в источнике с наибольшей полнотой.
- `year`: из приоритетной даты публикации; при расхождении берётся год от источника, предоставившего `doi`.
- Политика отказа: источник понижается, если `doi` некорректен или отсутствует минимальный набор (`title` и `date|year`).

**Таргеты (`targets`):**
- Номенклатура: `name`, `gene_symbol`, `organism` из UniProt; при отсутствии — из ChEMBL.
- Классификация/семейство: IUPHAR > ChEMBL.
- Отказ: если `uniprot_accession` некорректен, унификация сводится к ChEMBL, поле помечается как «требует уточнения».

**Ассайы (`assays`):**
- `assay_type`/`category`/`format`: соответствие BAO; при конфликте BAO-карта перекрывает `assay_type` из сырых данных ChEMBL.
- Привязки к `document_chembl_id` и `target_chembl_id` обязательны, если указаны в ChEMBL.

**Тест-айтемы (`testitems`):**
- Имена/синонимы: PubChem > ChEMBL.
- `salt` и `parent`-связи: пересечение карт соответствий; при конфликте отбрасывается источник без валидационного признака (например, отсутствие подтверждения `parent`).

**Активности (`activities`):**
- `standard_type`, `standard_units`, `standard_value`: выбирается запись с корректной единицей, требующей минимального преобразования к целевым; если есть `pchembl_value` и валидные исходные параметры, он сохраняется.
- При равной уверенности tie-breaker по полноте обязательных полей и свежести релиза источника (см. SRC-04).

**Общие правила:**
- Документы: при конфликтах полей применяется таблица приоритетов источников; решение конфликта журналируется.
- Молекулы/таргеты: join только по нормализованным ключам; несоответствия не сливаются молча.
- Энрихеры: join-ключи заданы явно; выполнение не нарушает детерминизм.

8. Минимальный реестр источников и требования к карточкам

Ниже указан обязательный состав полей карточек и опорные спецификации API для проверки контрактов.

8.1 Crossref

public_api: CrossrefPipeline в pipeline.py

config_keys: api_base_url, mailto, retries, backoff, rate_limit_rps, page_size, filters

entities: документы и связанное библиографическое метадано, включая авторов, лицензии, ссылки

merge_policy: join по doi_norm

tests_required: unit (client/parser/normalizer/schema), e2e с golden

risks: вариативный container-title, неоднородные авторские форматы
Подтверждающие спецификации: Crossref REST API, Swagger, рекомендации по использованию. 
GitHub
+4
www.crossref.org
+4
api.crossref.org
+4

8.2 PubMed E-utilities (NCBI Entrez)

public_api: PubMedPipeline

config_keys: api_base_url, api_key?, retries, backoff, page_size, rettype, retmode

entities: документы PubMed, идентификаторы PMID, поля библиографии

merge_policy: pmid → document_id и/или join с doi_norm

tests_required: unit+e2e; проверка батч-паттерна esearch → efetch

risks: лимиты, пагинация, неполные DOI
Опора: страницы NCBI об E-utilities и обзорные материалы. 
joshuadull.github.io
+4
ncbi.nlm.nih.gov
+4
ncbi.nlm.nih.gov
+4

8.3 OpenAlex

public_api: OpenAlexPipeline

config_keys: api_base_url, mailto, per_page, cursor, filters

entities: works, authors, источники, организации, связи работ

merge_policy: join по doi_norm/openalex_id

tests_required: unit+e2e; проверка курсорной пагинации и стабилизации ID

risks: неполные/рассинхроненные DOI
Опора: OpenAlex overview, API overview, Works/Authors объекты. 
OpenAlex
+4
OpenAlex
+4
OpenAlex
+4

8.4 Semantic Scholar

public_api: SemanticScholarPipeline

config_keys: api_base_url, api_key, rate_limit_rps, fields, page_size

entities: papers, citations/refs, идентификаторы paperId и DOI

merge_policy: join по doi_norm/paperId

tests_required: unit+e2e; проверка лимитов и усечения полей

risks: нестабильные поля ссылок, ограничения размера ответа
Опора: официальные API-страницы и лимиты/ограничения. 
Semantic Scholar API
+2
Semantic Scholar
+2

8.5 ChEMBL

public_api: ChEMBLPipeline

config_keys: api_base_url, page_size, retries, filters

entities: molecules, targets, assays, activities; поддержка фильтров и пагинации

merge_policy: molecule_chembl_id/assay_id

tests_required: unit+e2e; golden на свод единиц и преобразования standard_value

risks: неоднородность единиц активностей, большие ответы
Опора: ChEMBL Web Services и training-материалы. 
chembl.gitbook.io
+2
chembl.github.io
+2

8.6 UniProt

public_api: UniProtPipeline

config_keys: api_base_url, query, fields, format, size

entities: protein entries, маппинги, поля по fields

merge_policy: по uniprot_ac (c отделением версии) и/или маппинг-сервис

tests_required: unit+e2e; проверка полей и HTTP-заголовков/статусов

risks: различия версий AC, изменение схемы полей
Опора: UniProt REST API и справка по query/headers/id-mapping. 
UniProt
+3
UniProt
+3
UniProt
+3

8.7 PubChem

public_api: PubChemPipeline

config_keys: endpoint, namespace, identifiers, batch_size

entities: compounds/substances/assays по PUG-REST/PUG-View

merge_policy: по cid/sid

tests_required: unit+e2e; проверка пакетных запросов и стабильности полей

risks: объём ответов, ограничения по структурам/поиску
Опора: PUG-REST и PUG-View. 
PubChem
+2
PubChem
+2

8.8 IUPHAR/BPS Guide to PHARMACOLOGY (GtoP)

public_api: GtoPPipeline

config_keys: api_base_url, page, per_page, api_key

entities: targets, genes, ligands, interactions

merge_policy: ligand_id/target_id и маппинги на UniProt/HGNC

tests_required: unit+e2e; проверка целостности маппингов

risks: частичные поля, обновления структуры JSON
Опора: веб-сервисы GtoP и публикации-описания. 
pmc.ncbi.nlm.nih.gov
+3
guidetopharmacology.org
+3
guidetopharmacology.org
+3

9. Детальные требования к источникам данных (SRC-*)

Требования к перечню источников и их покрытию полей сущностей.

**SRC-01 (MUST): Покрытие источников**

Для каждой сущности должен быть зафиксирован перечень источников (основной и дополнительные) с указанием, какие поля покрываются каждым источником. Все обязательные поля сущности покрыты хотя бы одним источником; «дыры покрытия» отсутствуют.

**Метод проверки:** Сквозная валидация покрытия по карте «поле → источник» и перечню обязательных полей (отчёт о непросуммированных полях пуст).

**Источники:** [ref: repo:docs/requirements/99-data-sources-and-data-spec.md]

**SRC-02 (MUST): Канонизация идентификаторов**

Идентификаторы входа приводятся к канону:
- `CHEMBL\d+` для ChEMBL
- `10.*` для DOI (без префиксов `doi:`/URL, в нижнем регистре доменной части)
- `^\d+$` для PMID
- Канон UniProt accession (верхний регистр, допустимая длина/символы)

**Метод проверки:** Регекс-валидация на уровне колонок; отчёты по несоответствиям.

**Источники:** [ref: repo:docs/requirements/04-normalization-validation.md], [ref: repo:src/bioetl/schemas/]

**SRC-03 (MUST): Критерии включения/исключения**

Определены критерии включения/исключения записей по каждому источнику: минимально обязательные поля, валидные диапазоны значений, перечень неустранимых противоречий. Нарушения отражаются в отчёте исключений с причиной.

**Метод проверки:** Фильтрация по правилам включения и агрегация причин исключения.

**Источники:** [ref: repo:docs/requirements/03-data-extraction.md], [ref: repo:docs/requirements/04-normalization-validation.md]

**SRC-04 (SHOULD): Атрибуты происхождения**

Для каждого источника фиксируются атрибуты происхождения значений: `source_name`, `source_release`/`version`, `retrieved_date` (ISO 8601). В результирующих наборах присутствуют и заполнены поля происхождения для всех значений из внешних источников.

**Метод проверки:** Наличие и ненулевость полей происхождения; кросс-проверка допустимых значений версий источников.

**Источники:** [ref: repo:docs/requirements/03-data-extraction.md]

10. Детальные требования к данным (DATA-*)

Требования к схемам, типам, валидации и качеству данных.

**DATA-01 (MUST): Перечень полей с классификацией**

Для каждой сущности задан перечень полей с классификацией: обязательные, опциональные, вычислимые/нормализуемые (ID, даты, единицы, метки онтологий). Ни одно обязательное поле не пустое; опциональные пустуют только при документированном основании.

**Метод проверки:** Контроль ненулевости и верификация оснований.

**Источники:** [ref: repo:docs/requirements/05-assay-extraction.md], [ref: repo:docs/requirements/06-activity-data-extraction.md], [ref: repo:docs/requirements/07a-testitem-extraction.md], [ref: repo:docs/requirements/07b-testitem-data-extraction.md], [ref: repo:docs/requirements/08-target-data-extraction.md]

**DATA-02 (MUST): Типы данных и единицы измерения**

Типы данных и единицы измерения определены для всех числовых и датовых полей; даты в ISO 8601; для активностей зафиксирован словарь допустимых метрик (`standard_type`) и единиц (`standard_units`) с валидными диапазонами.

**Метод проверки:** Схемы, диапазоны, допустимые значения.

**Источники:** [ref: repo:src/bioetl/schemas/], [ref: repo:docs/requirements/06-activity-data-extraction.md]

**DATA-03 (MUST): Канонизированные ключи связей**

Канонизированы ключи связей между сущностями: `document_chembl_id`, `assay_chembl_id`, `molecule_chembl_id`, `target_chembl_id`, `uniprot_accession` (если применимо). Все ссылки разрешаются однозначно; дубликаты и конфликтующие ключи фиксируются.

**Метод проверки:** Контроль внешних ключей и целостности.

**Источники:** [ref: repo:docs/requirements/04-normalization-validation.md]

**DATA-04 (MUST): Дедупликация и консолидация**

Заданы правила дедупликации и консолидации записей: критерии эквивалентности, приоритеты полей при слиянии, разрешение противоречий значений между источниками. После консолидации отсутствуют точные дубликаты; каждая конфликтная запись проходит детерминированный выбор.

**Метод проверки:** Поиск дублей по бизнес-ключам и контроль приоритетов.

**Источники:** [ref: repo:docs/requirements/03-data-extraction.md], [ref: repo:docs/requirements/04-normalization-validation.md]

**DATA-05 (SHOULD): Онтологическая привязка**

Поля, требующие онтологической привязки, валидируются по контролируемым словарям: BAO для `assay_*`, UniProt/таксономия для таргетов, словарь метрик и единиц для активностей.

**Метод проверки:** Сопоставление значений со словарями и отчёт о несоответствиях.

**Источники:** [ref: repo:docs/requirements/05-assay-extraction.md], [ref: repo:docs/requirements/08-target-data-extraction.md]

**DATA-06 (MUST): Политика пропусков**

Определено, где допускается `NULL`, где пустая строка запрещена; различается «структурно отсутствующее» (нет в источнике) и «данные отсутствуют» (источник сообщает `unknown`). Отчёт подтверждает отсутствие запрещённых пропусков.

**Метод проверки:** Валидаторы схем и агрегирование пропусков по классу причин.

**Источники:** [ref: repo:docs/requirements/04-normalization-validation.md]

**DATA-07 (SHOULD): Гармонизация единиц и шкал**

Гармонизация единиц и шкал: целевые единицы заданы для измеримых полей (например, активности в нМ, время в часах); допускается расчёт производных показателей (`pChEMBL`) при наличии исходных значений. Итоговые таблицы имеют единые единицы; для каждой числовой колонки указан целевой юнит.

**Метод проверки:** Скан колонок на предмет единственного unit; отчёт о смешанных единицах пуст.

**Источники:** [ref: repo:docs/requirements/06-activity-data-extraction.md], [ref: repo:src/bioetl/schemas/]

11. Требования к целостности данных (LINK-*)

**LINK-01 (MUST): Разрешение ссылок**

`activities` ссылаются на существующие `assays`, `documents`, `testitems`, `targets` по каноническим ключам. Неразрешённых ссылок быть не должно.

**Метод проверки:** Внешние ключи и полнота джойнов.

**Источники:** [ref: repo:docs/requirements/04-normalization-validation.md]

**LINK-02 (SHOULD): Кардинальности и уникальность**

Заданы кардинальности и уникальные бизнес-ключи:
- Для `assays` уникален `assay_chembl_id`
- Для `documents` — `document_chembl_id`
- Для `activities` — комбинация (`assay_chembl_id`, `molecule_chembl_id`, `standard_type`, `relation`, дополнительные измерения при наличии)

Нарушения уникальности отсутствуют; кардинальности не противоречат данным.

**Метод проверки:** Агрегаты уникальности.

**Источники:** [ref: repo:src/bioetl/schemas/]

12. Критерии приёмки (проверяемые метрики)

| ID | Проверка | Метод | Ожидаемо | Evidence |
| --- | --- | --- | --- | --- |
| SRC-02 | Формат всех ID соответствует канону | Регекс-валидация колонок (`CHEMBL\d+`, `^10\..+`, `^\d+$`, канон UniProt) | 100% валид; отчёт несоответствий пуст | Отчёт валидации идентификаторов |
| DATA-01 | Обязательные поля не пусты | Проверка ненулевости по списку обязательных полей | 0 нарушений | Сводка ненулевости по сущностям |
| DATA-02 | Единицы и типы корректны | Типовые проверки схем, списки допустимых значений, диапазоны | 0 нарушений или помечены флагами качества | Отчёт по типам/единицам |
| DATA-03 / LINK-01 | Все ссылки разрешены | Полные джойны справочников, подсчёт «осиротевших» ссылок | 0 неразрешённых ссылок | Отчёт целостности ссылок |
| DATA-04 | Отсутствуют точные дубликаты | Проверка уникальности по бизнес-ключам | 0 дублей | Сводка уникальности |
| DATA-05 | Значения соответствуют словарям/онтологиям | Сопоставление со словарями BAO/UniProt/метрики активностей | 100% в словарях или в реестре несоответствий | Реестр несоответствий |
| DATA-06 | Политика пропусков соблюдена | Классифицированный анализ пропусков | Нет запрещённых пропусков | Отчёт пропусков |
| DATA-07 | Гармонизация единиц завершена | Проверка единственности целевого юнита по колонке | Все числовые поля в целевых единицах | Отчёт по единицам |

📄 **Полное описание**: [docs/requirements/99-data-sources-and-data-spec.md](../docs/requirements/99-data-sources-and-data-spec.md)

13. Матрица трассируемости требований

| Требование | Ссылки на документы |
| --- | --- |
| SRC-01 | [ref: repo:docs/requirements/99-data-sources-and-data-spec.md] |
| SRC-02 | [ref: repo:docs/requirements/04-normalization-validation.md], [ref: repo:src/bioetl/schemas/] |
| SRC-03 | [ref: repo:docs/requirements/03-data-extraction.md], [ref: repo:docs/requirements/04-normalization-validation.md] |
| SRC-04 | [ref: repo:docs/requirements/03-data-extraction.md] |
| DATA-01 | [ref: repo:docs/requirements/05-assay-extraction.md], [ref: repo:docs/requirements/06-activity-data-extraction.md], [ref: repo:docs/requirements/07a-testitem-extraction.md], [ref: repo:docs/requirements/07b-testitem-data-extraction.md], [ref: repo:docs/requirements/08-target-data-extraction.md] |
| DATA-02 | [ref: repo:src/bioetl/schemas/], [ref: repo:docs/requirements/06-activity-data-extraction.md] |
| DATA-03 | [ref: repo:docs/requirements/04-normalization-validation.md] |
| DATA-04 | [ref: repo:docs/requirements/03-data-extraction.md], [ref: repo:docs/requirements/04-normalization-validation.md] |
| DATA-05 | [ref: repo:docs/requirements/05-assay-extraction.md], [ref: repo:docs/requirements/08-target-data-extraction.md] |
| DATA-06 | [ref: repo:docs/requirements/04-normalization-validation.md] |
| DATA-07 | [ref: repo:docs/requirements/06-activity-data-extraction.md], [ref: repo:src/bioetl/schemas/] |
| LINK-01 | [ref: repo:docs/requirements/04-normalization-validation.md] |
| LINK-02 | [ref: repo:src/bioetl/schemas/] |

14. Требования к тестированию (MUST)

Unit-тесты для клиента, парсера, нормализатора и схемы на каждый источник.

E2E-тест с golden-сэмплом; проверяются: количество строк, уникальность ключей, доля NA, валидность единиц, стабильный порядок колонок и сериализация.

Тест идемпотентности на повторный прогон с теми же параметрами.

10. Критерии приемки (MUST)

SOURCES_AND_INTERFACES.md соответствует дереву test_refactoring_11; расхождений нет.

Нормализация и валидация описаны так, что по тексту можно восстановить схемы без догадок.

Merge-ключи и приоритизация источников определены; конфликты детерминизированы и журналируются.

Все новые/уточнённые правила покрыты unit+e2e; golden-наборы обновлены.

Любые изменения перечня источников отражены одновременно в REFACTOR_PLAN.md и SOURCES_AND_INTERFACES.md в одном PR.

11. Примечания по API-лимитам и пагинации (SHOULD)

Карточки источников SHOULD фиксировать лимиты и пагинацию, если они документированы провайдерами: OpenAlex лимиты в техдоке; Semantic Scholar ограничения размера и частоты; ChEMBL пагинация; PubMed паттерн esearch/efetch. Эти сведения обязательны для настройки retries, backoff и rate_limit_rps в config_keys. 
ncbi.nlm.nih.gov
+4
OpenAlex
+4
Semantic Scholar API
+4

12. Язык требований

В тексте перечня допускается только терминология из спецификаций соответствующих API и RFC 2119. Любые «вольные» поля и незафиксированные преобразования запрещены (MUST NOT). 
ietf.org

