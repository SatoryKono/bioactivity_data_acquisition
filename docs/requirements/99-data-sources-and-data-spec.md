# Спецификация источников данных и требований к данным

## Область действия и термины

Документ фиксирует требования к источникам данных и самим данным для сущностей `documents`, `targets`, `assays`, `testitems`, `activities`. Реализационные детали (CLI, пайплайны, ретраи, логирование, кэш, хранилища и т.п.) исключены из области.

**Термины**

- **Основной источник** — источник, из которого формируется базовый набор полей сущности.
- **Дополнительный источник (обогащение)** — источник, используемый для дополнения или уточнения отдельных полей при наличии соответствия по идентификаторам.
- **Бизнес-ключ** — минимальный набор полей, однозначно идентифицирующий запись на уровне предметной области.
- **Канонизация идентификаторов** — приведение идентификаторов к нормативной форме (регистр, префиксы, формат).
- **Контролируемые словари/онтологии** — фиксированные перечни допустимых значений или ссылочные онтологии.


**Контекст проекта.** ETL ориентирован на извлечение биоактивностных данных из ChEMBL и внешних источников с последующей нормализацией и проверками схем. Контекст подтверждён структурой репозитория (каталоги `schemas/`, `pipelines/`).

## Матрица источников по сущностям

| Сущность | Основной источник | Доп. источники (обогащение) | Обязательные поля (уровень данных) | Идентификаторы/ключи (формат) | Онтологии/словари | Политика слияния (кратко) |
| --- | --- | --- | --- | --- | --- | --- |
| `documents` | ChEMBL Documents API | PubMed, Crossref, OpenAlex, Semantic Scholar | `document_chembl_id`, `title`, `year` или `date`, хотя бы один из `doi`/`pmid` | БК: `document_chembl_id`; внешние: `doi` (канонический `10.*`), `pmid` (целое) | — | Базовые поля из ChEMBL; `doi`/`title`/`venue`/`date` при конфликте: Crossref > PubMed > OpenAlex > ChEMBL; авторы: PubMed > Crossref; ссылки агрегируются с дедупликацией |
| `targets` | ChEMBL Target API | UniProt, IUPHAR (Guide to Pharmacology) | `target_chembl_id`, `organism`, `target_type`; при наличии — `uniprot_accession`, `gene_symbol` | БК: `target_chembl_id`; внешние: `uniprot_accession` | Таксономия NCBI (для `organism`), номенклатура UniProt (аккессия, ген) | Маркерные поля (`name`, `gene_symbol`, `organism`) при конфликте: UniProt > ChEMBL; классификация мишени: IUPHAR > ChEMBL |
| `assays` | ChEMBL Assay API | BAO (онтология биологических ассайев) | `assay_chembl_id`, `assay_type`, `assay_category`, `target_chembl_id?`, `document_chembl_id?` | БК: `assay_chembl_id` | BAO для `assay_type`/`category`/`format` | Тип/категория нормализуются к BAO; при расхождении: BAO-карта > ChEMBL raw |
| `testitems` | ChEMBL Molecule API | PubChem (идентификаторы, вещества/соли, синонимы) | `molecule_chembl_id`; при наличии — `preferred_name`, `synonyms[]`, `parent`, `salt_id` | БК: `molecule_chembl_id`; внешние: PubChem CID | — | Имена/синонимы: PubChem > ChEMBL; соль/`parent` согласуются по карте соответствий; конфликты помечаются |
| `activities` | ChEMBL Activity API | — (нормативы единиц и метрик из схем) | `activity_id?`, `assay_chembl_id`, `molecule_chembl_id`, `standard_type`, `standard_relation`, `standard_value`, `standard_units`; опц.: `pchembl_value` | БК: комбинация (`assay_chembl_id`, `molecule_chembl_id`, `standard_type`, `relation`, `timepoint?`) | Словарь допустимых метрик (IC50, EC50, Ki и др.); словарь единиц (нМ, мкМ и т.п.) | Единицы гармонизируются к целевым (см. DATA-07); при конфликтных измерениях выбирается запись с валидной единицей, меньшей степенью преобразования, лучшим признаком качества |

Ссылки на конкретные разделы ветки приведены в разделе «Трассируемость».

## Требования к источникам данных (SRC-*)

- **SRC-01 (MUST).** Для каждой сущности должен быть зафиксирован перечень источников (основной и дополнительные) с указанием, какие поля покрываются каждым источником. Все обязательные поля сущности покрыты хотя бы одним источником; «дыры покрытия» отсутствуют. Проверка: сквозная валидация покрытия по карте «поле → источник» и перечню обязательных полей (отчёт о непросуммированных полях пуст). Источники: [ref: repo:docs/ @test_refactoring_11], [ref: repo:docs/requirements/03-data-extraction.md @test_refactoring_11].
- **SRC-02 (MUST).** Идентификаторы входа приводятся к канону: `CHEMBL\d+` для ChEMBL, `10.*` для DOI (без префиксов `doi:`/URL, в нижнем регистре доменной части, инвариантная часть после `10.` без пробелов), `^\d+$` для PMID, канон UniProt accession (верхний регистр, допустимая длина/символы). Покрытие 100%; некорректные значения попадают в список отбраковки. Проверка: регекс-валидация на уровне колонок; отчёты по несоответствиям. Источники: [ref: repo:docs/requirements/04-normalization-validation.md @test_refactoring_11], [ref: repo:src/bioetl/schemas/ @test_refactoring_11].
- **SRC-03 (MUST).** Определены критерии включения/исключения записей по каждому источнику: минимально обязательные поля, валидные диапазоны значений, перечень неустранимых противоречий. Нарушения отражаются в отчёте исключений с причиной. Проверка: фильтрация по правилам включения и агрегация причин исключения. Источники: [ref: repo:docs/requirements/03-data-extraction.md @test_refactoring_11], [ref: repo:docs/requirements/04-normalization-validation.md @test_refactoring_11].
- **SRC-04 (SHOULD).** Для каждого источника фиксируются атрибуты происхождения значений: `source_name`, `source_release`/`version`, `retrieved_date` (ISO 8601). В результирующих наборах присутствуют и заполнены поля происхождения для всех значений из внешних источников. Проверка: наличие и ненулевость полей происхождения; кросс-проверка допустимых значений версий источников. Источник: [ref: repo:docs/requirements/03-data-extraction.md @test_refactoring_11].


## Требования к данным и схемам (DATA-*)

- **DATA-01 (MUST).** Для каждой сущности задан перечень полей с классификацией: обязательные, опциональные, вычислимые/нормализуемые (ID, даты, единицы, метки онтологий). Ни одно обязательное поле не пустое; опциональные пустуют только при документированном основании. Проверка: контроль ненулевости и верификация оснований. Источники: [ref: repo:docs/requirements/05-assay-extraction.md @test_refactoring_11], [ref: repo:docs/requirements/06-activity-data-extraction.md @test_refactoring_11], [ref: repo:docs/requirements/07a-testitem-extraction.md @test_refactoring_11], [ref: repo:docs/requirements/07b-testitem-data-extraction.md @test_refactoring_11], [ref: repo:docs/requirements/08a-target-chembl-extraction.md @test_refactoring_11].
- **DATA-02 (MUST).** Типы данных и единицы измерения определены для всех числовых и датовых полей; даты в ISO 8601; для активностей зафиксирован словарь допустимых метрик (`standard_type`) и единиц (`standard_units`) с валидными диапазонами. Проверка: схемы, диапазоны, допустимые значения. Источники: [ref: repo:src/bioetl/schemas/ @test_refactoring_11], [ref: repo:docs/requirements/06-activity-data-extraction.md @test_refactoring_11].
- **DATA-03 (MUST).** Канонизированы ключи связей между сущностями: `document_chembl_id`, `assay_chembl_id`, `molecule_chembl_id`, `target_chembl_id`, `uniprot_accession` (если применимо). Все ссылки разрешаются однозначно; дубликаты и конфликтующие ключи фиксируются. Проверка: контроль внешних ключей и целостности. Источник: [ref: repo:docs/requirements/04-normalization-validation.md @test_refactoring_11].
- **DATA-04 (MUST).** Заданы правила дедупликации и консолидации записей: критерии эквивалентности, приоритеты полей при слиянии, разрешение противоречий значений между источниками. После консолидации отсутствуют точные дубликаты; каждая конфликтная запись проходит детерминированный выбор. Проверка: поиск дублей по бизнес-ключам и контроль приоритетов. Источники: [ref: repo:docs/requirements/03-data-extraction.md @test_refactoring_11], [ref: repo:docs/requirements/04-normalization-validation.md @test_refactoring_11].
- **DATA-05 (SHOULD).** Поля, требующие онтологической привязки, валидируются по контролируемым словарям: BAO для `assay_*`, UniProt/таксономия для таргетов, словарь метрик и единиц для активностей. Проверка: сопоставление значений со словарями и отчёт о несоответствиях. Источники: [ref: repo:docs/requirements/05-assay-extraction.md @test_refactoring_11], [ref: repo:docs/requirements/08a-target-chembl-extraction.md @test_refactoring_11].
- **DATA-06 (MUST).** Политика пропусков: определено, где допускается `NULL`, где пустая строка запрещена; различается «структурно отсутствующее» (нет в источнике) и «данные отсутствуют» (источник сообщает `unknown`). Отчёт подтверждает отсутствие запрещённых пропусков. Проверка: валидаторы схем и агрегирование пропусков по классу причин. Источник: [ref: repo:docs/requirements/04-normalization-validation.md @test_refactoring_11].
- **DATA-07 (SHOULD).** Гармонизация единиц и шкал: целевые единицы заданы для измеримых полей (например, активности в нМ, время в часах); допускается расчёт производных показателей (`pChEMBL`) при наличии исходных значений. Итоговые таблицы имеют единые единицы; для каждой числовой колонки указан целевой юнит. Проверка: скан колонок на предмет единственного unit; отчёт о смешанных единицах пуст. Источники: [ref: repo:docs/requirements/06-activity-data-extraction.md @test_refactoring_11], [ref: repo:src/bioetl/schemas/ @test_refactoring_11].


## Приоритезация, слияние и разрешение конфликтов

- **Документы (`documents`).**
  - `doi`, `title`, `container_title`/`journal`, `published_(print|online)_date`: Crossref > PubMed > OpenAlex > ChEMBL.
  - `authors`: PubMed > Crossref; дедупликация по (`surname`, `initials`); порядок как в источнике с наибольшей полнотой.
  - `year`: из приоритетной даты публикации; при расхождении берётся год от источника, предоставившего `doi`.
  - Политика отказа: источник понижается, если `doi` некорректен или отсутствует минимальный набор (`title` и `date|year`).
- **Таргеты (`targets`).**
  - Номенклатура: `name`, `gene_symbol`, `organism` из UniProt; при отсутствии — из ChEMBL.
  - Классификация/семейство: IUPHAR > ChEMBL.
  - Отказ: если `uniprot_accession` некорректен, унификация сводится к ChEMBL, поле помечается как «требует уточнения».
- **Ассайы (`assays`).**
  - `assay_type`/`category`/`format`: соответствие BAO; при конфликте BAO-карта перекрывает `assay_type` из сырых данных ChEMBL.
  - Привязки к `document_chembl_id` и `target_chembl_id` обязательны, если указаны в ChEMBL.
- **Тест-айтемы (`testitems`).**
  - Имена/синонимы: PubChem > ChEMBL.
  - `salt` и `parent`-связи: пересечение карт соответствий; при конфликте отбрасывается источник без валидационного признака (например, отсутствие подтверждения `parent`).
- **Активности (`activities`).**
  - `standard_type`, `standard_units`, `standard_value`: выбирается запись с корректной единицей, требующей минимального преобразования к целевым; если есть `pchembl_value` и валидные исходные параметры, он сохраняется.
  - При равной уверенности tie-breaker по полноте обязательных полей и свежести релиза источника (см. SRC-04).


## Связи и целостность данных (LINK-*)

- **LINK-01 (MUST).** `activities` ссылаются на существующие `assays`, `documents`, `testitems`, `targets` по каноническим ключам. Неразрешённых ссылок быть не должно. Проверка: внешние ключи и полнота джойнов. Источник: [ref: repo:docs/requirements/04-normalization-validation.md @test_refactoring_11].
- **LINK-02 (SHOULD).** Заданы кардинальности и уникальные бизнес-ключи: для `assays` уникален `assay_chembl_id`; для `documents` — `document_chembl_id`; для `activities` — комбинация (`assay_chembl_id`, `molecule_chembl_id`, `standard_type`, `relation`, дополнительные измерения при наличии). Нарушения уникальности отсутствуют; кардинальности не противоречат данным. Проверка: агрегаты уникальности. Источник: [ref: repo:src/bioetl/schemas/ @test_refactoring_11].


## Критерии приемки (проверки данных)

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

## Трассируемость: матрица «требование → ссылка на документ ветки»

| Требование | Ссылки |
| --- | --- |
| SRC-01 | [ref: repo:docs/ @test_refactoring_11]; [ref: repo:docs/requirements/03-data-extraction.md @test_refactoring_11] |
| SRC-02 | [ref: repo:docs/requirements/04-normalization-validation.md @test_refactoring_11]; [ref: repo:src/bioetl/schemas/ @test_refactoring_11] |
| SRC-03 | [ref: repo:docs/requirements/03-data-extraction.md @test_refactoring_11]; [ref: repo:docs/requirements/04-normalization-validation.md @test_refactoring_11] |
| SRC-04 | [ref: repo:docs/requirements/03-data-extraction.md @test_refactoring_11] |
| DATA-01 | [ref: repo:docs/requirements/05-assay-extraction.md @test_refactoring_11]; [ref: repo:docs/requirements/06-activity-data-extraction.md @test_refactoring_11]; [ref: repo:docs/requirements/07a-testitem-extraction.md @test_refactoring_11]; [ref: repo:docs/requirements/07b-testitem-data-extraction.md @test_refactoring_11]; [ref: repo:docs/requirements/08a-target-chembl-extraction.md @test_refactoring_11] |
| DATA-02 | [ref: repo:src/bioetl/schemas/ @test_refactoring_11]; [ref: repo:docs/requirements/06-activity-data-extraction.md @test_refactoring_11] |
| DATA-03 | [ref: repo:docs/requirements/04-normalization-validation.md @test_refactoring_11] |
| DATA-04 | [ref: repo:docs/requirements/03-data-extraction.md @test_refactoring_11]; [ref: repo:docs/requirements/04-normalization-validation.md @test_refactoring_11] |
| DATA-05 | [ref: repo:docs/requirements/05-assay-extraction.md @test_refactoring_11]; [ref: repo:docs/requirements/08a-target-chembl-extraction.md @test_refactoring_11] |
| DATA-06 | [ref: repo:docs/requirements/04-normalization-validation.md @test_refactoring_11] |
| DATA-07 | [ref: repo:docs/requirements/06-activity-data-extraction.md @test_refactoring_11]; [ref: repo:src/bioetl/schemas/ @test_refactoring_11] |
| LINK-01 | [ref: repo:docs/requirements/04-normalization-validation.md @test_refactoring_11] |
| LINK-02 | [ref: repo:src/bioetl/schemas/ @test_refactoring_11] |

## Нотация

Требования сформулированы по RFC 2119 с ключевыми словами MUST, SHOULD, MAY. Поля и сущности названы согласно материалам ветки `test_refactoring_11`. Общий контекст работы с ChEMBL и внешними источниками подтверждён описанием репозитория и структурой каталогов (`schemas/`, `pipelines/`).
