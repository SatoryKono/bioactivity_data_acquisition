# Requirements Audit

## Введение

В ходе аудита проанализированы требования и вспомогательные материалы ветки `@test_refactoring_32` репозитория `SatoryKono/bioactivity_data_acquisition`. Основное внимание уделялось спецификациям компонентов UnifiedArchitecture, профилям пайплайнов (assay, activity, testitem, target, document) и сопутствующим документам конфигурации, логирования, ввода-вывода и выявленных разрывов качества. Метод включал перекрестное сравнение требований между модулями, сопоставление с gap-листом и оценку полноты контрактов источников, политик детерминизма и схем данных. [ref: repo:docs/requirements/00-architecture-overview.md@test_refactoring_32] [ref: repo:docs/requirements/05-assay-extraction.md@test_refactoring_32] [ref: repo:docs/requirements/06-activity-data-extraction.md@test_refactoring_32] [ref: repo:docs/requirements/07a-testitem-extraction.md@test_refactoring_32] [ref: repo:docs/requirements/08-target-data-extraction.md@test_refactoring_32] [ref: repo:docs/requirements/09-document-chembl-extraction.md@test_refactoring_32] [ref: repo:docs/requirements/10-configuration.md@test_refactoring_32] [ref: repo:docs/gaps.md@test_refactoring_32]

## Инвентаризация артефактов

| Артефакт | Назначение | Ссылка |
|---|---|---|
| Архитектурный обзор UnifiedArchitecture | Принципы детерминизма, компоновка подсистем | [ref: repo:docs/requirements/00-architecture-overview.md@test_refactoring_32] |
| UnifiedLogger | Контракты структурированного логирования и обязательные поля контекста | [ref: repo:docs/requirements/01-logging-system.md@test_refactoring_32] |
| UnifiedOutputWriter | Политики атомарной записи, QC, форматы артефактов | [ref: repo:docs/requirements/02-io-system.md@test_refactoring_32] |
| UnifiedAPIClient | Слои устойчивости HTTP, кэширование, retry-after | [ref: repo:docs/requirements/03-data-extraction.md@test_refactoring_32] |
| UnifiedSchema | Нормализаторы, Pandera-схемы и NA/precision policy | [ref: repo:docs/requirements/04-normalization-validation.md@test_refactoring_32] |
| Assay Pipeline | Детализация стадий ETL для ассая | [ref: repo:docs/requirements/05-assay-extraction.md@test_refactoring_32] |
| Activity Pipeline | Контракт извлечения активностей, маппинг и QC | [ref: repo:docs/requirements/06-activity-data-extraction.md@test_refactoring_32] |
| Testitem Pipeline | Извлечение молекул, PubChem enrichment, детерминизм | [ref: repo:docs/requirements/07a-testitem-extraction.md@test_refactoring_32] |
| PubChem Integration Guide | Глубокие контракты PubChem для testitem | [ref: repo:docs/requirements/07b-testitem-data-extraction.md@test_refactoring_32] |
| Target Pipeline | ChEMBL + UniProt + IUPHAR поток и выходные таблицы | [ref: repo:docs/requirements/08-target-data-extraction.md@test_refactoring_32] |
| Document Pipeline | Режимы `chembl`/`all`, адаптеры PubMed/Crossref/OpenAlex/S2 | [ref: repo:docs/requirements/09-document-chembl-extraction.md@test_refactoring_32] |
| Конфигурационный стандарт | Единая модель YAML/Pydantic, наследование профилей | [ref: repo:docs/requirements/10-configuration.md@test_refactoring_32] |
| Gap-лист | Текущие пробелы и риски требований | [ref: repo:docs/gaps.md@test_refactoring_32] |

## Диагностика проблем

### Контракты источников

- **Неустранённые «uncertain» параметры пагинации/лимитов в activity и document пайплайнах**: отсутствуют проверенные верхние лимиты `limit` и подтверждение набора полей `document.json`, что делает требования непроверяемыми. [ref: repo:docs/requirements/06-activity-data-extraction.md@test_refactoring_32] [ref: repo:docs/requirements/09-document-chembl-extraction.md@test_refactoring_32]
- **Разнородное описание batch-size ограничений**: Assay/Testitem/Document фиксируют `≤25` в тексте, но Activity опирается на gap G3 без явного AC. [ref: repo:docs/requirements/05-assay-extraction.md@test_refactoring_32] [ref: repo:docs/requirements/07a-testitem-extraction.md@test_refactoring_32] [ref: repo:docs/requirements/09-document-chembl-extraction.md@test_refactoring_32] [ref: repo:docs/gaps.md@test_refactoring_32]

### Детерминизм и нормализация

- **column_order и NA-policy объявлены в разных файлах без общего источника истины** (gaps G4/G5), meta.yaml копирует порядок вместо ссылки на схему. [ref: repo:docs/requirements/02-io-system.md@test_refactoring_32] [ref: repo:docs/requirements/04-normalization-validation.md@test_refactoring_32] [ref: repo:docs/gaps.md@test_refactoring_32]
- **Ассиметрия сортировок**: Activity требует сортировки по `activity_id`, Assay — по `assay_chembl_id,row_subtype,row_index`, остальные пайплайны не закрепляют порядок в явных AC. [ref: repo:docs/requirements/05-assay-extraction.md@test_refactoring_32] [ref: repo:docs/requirements/06-activity-data-extraction.md@test_refactoring_32] [ref: repo:docs/requirements/07a-testitem-extraction.md@test_refactoring_32] [ref: repo:docs/requirements/08-target-data-extraction.md@test_refactoring_32] [ref: repo:docs/requirements/09-document-chembl-extraction.md@test_refactoring_32]

### Схемы и маппинги

- **Отсутствие централизованных Pandera-схем для выходов**: перечисления колонок (например, ~95 полей Testitem) не связаны с конкретным schema-файлом, усложняя проверяемость. [ref: repo:docs/requirements/07a-testitem-extraction.md@test_refactoring_32] [ref: repo:docs/requirements/04-normalization-validation.md@test_refactoring_32]
- **Target pipeline** описывает четыре выходные таблицы, но не фиксирует первичные ключи в явном формате схемы (только текстовое описание). [ref: repo:docs/requirements/08-target-data-extraction.md@test_refactoring_32]

### CLI и конфигурация

- **Несогласованные CLI-инварианты**: Assay описывает новые флаги (`--golden`, `--sample`), тогда как Activity/Target/Document ссылаются на базовый CLI без конкретных контрактов. [ref: repo:docs/requirements/05-assay-extraction.md@test_refactoring_32] [ref: repo:docs/requirements/06-activity-data-extraction.md@test_refactoring_32] [ref: repo:docs/requirements/08-target-data-extraction.md@test_refactoring_32] [ref: repo:docs/requirements/09-document-chembl-extraction.md@test_refactoring_32]
- **Конфигурация batch-size** проверяется в тексте, но отсутствуют явные схемы в `PipelineConfig` для лимитов внешних источников (PubChem, UniProt, IUPHAR). [ref: repo:docs/requirements/07b-testitem-data-extraction.md@test_refactoring_32] [ref: repo:docs/requirements/08-target-data-extraction.md@test_refactoring_32] [ref: repo:docs/requirements/10-configuration.md@test_refactoring_32]

### QC и логирование

- **Обязательные поля логов описаны, но не привязаны к пайплайнам через AC** (gap G12). [ref: repo:docs/requirements/01-logging-system.md@test_refactoring_32] [ref: repo:docs/gaps.md@test_refactoring_32]
- **QC-пороги частично перечислены** (Assay/Activity), однако Document/Testitem/Target не закрепляют минимальные пороги пропусков/дубликатов. [ref: repo:docs/requirements/05-assay-extraction.md@test_refactoring_32] [ref: repo:docs/requirements/06-activity-data-extraction.md@test_refactoring_32] [ref: repo:docs/requirements/07a-testitem-extraction.md@test_refactoring_32] [ref: repo:docs/requirements/09-document-chembl-extraction.md@test_refactoring_32]

## Точечные исправления

1. **Зафиксировать проверенные лимиты пагинации и поля ответов** для Activity (`limit` через бинарный поиск) и Document (`document.json` field list) и оформить AC. [ref: repo:docs/requirements/06-activity-data-extraction.md@test_refactoring_32] [ref: repo:docs/requirements/09-document-chembl-extraction.md@test_refactoring_32]
2. **Вынести единый норматив column_order/NA-policy** в централизованную схему и ссылаться из всех пайплайнов вместо дублирования в meta.yaml. [ref: repo:docs/requirements/02-io-system.md@test_refactoring_32] [ref: repo:docs/requirements/04-normalization-validation.md@test_refactoring_32]
3. **Сформализовать Pandera OutputSchema** для Testitem/Target/Document с явными PK/FK и ссылкой на список колонок. [ref: repo:docs/requirements/07a-testitem-extraction.md@test_refactoring_32] [ref: repo:docs/requirements/08-target-data-extraction.md@test_refactoring_32] [ref: repo:docs/requirements/09-document-chembl-extraction.md@test_refactoring_32]
4. **Добавить унифицированные CLI-контракты** (поддерживаемые флаги, инварианты) в стандарт конфигурации/CLI, а не только в отдельных спецификациях. [ref: repo:docs/requirements/05-assay-extraction.md@test_refactoring_32] [ref: repo:docs/requirements/10-configuration.md@test_refactoring_32]
5. **Включить обязательные поля логов и QC-пороги** в Acceptance Criteria пайплайнов для проверяемости. [ref: repo:docs/requirements/01-logging-system.md@test_refactoring_32] [ref: repo:docs/requirements/05-assay-extraction.md@test_refactoring_32] [ref: repo:docs/requirements/06-activity-data-extraction.md@test_refactoring_32] [ref: repo:docs/gaps.md@test_refactoring_32]

## Матрица трассируемости

| requirement_id | description | file_section | artifact | verification | evidence |
|---|---|---|---|---|---|
| AUD-1 | Требуется подтвердить лимиты пагинации/ответов ChEMBL Activity/Document | Activity §2, Document §2-3 | План тестирования лимитов | Бинарный поиск limit + curl записи | [ref: repo:docs/requirements/06-activity-data-extraction.md@test_refactoring_32] [ref: repo:docs/requirements/09-document-chembl-extraction.md@test_refactoring_32] |
| AUD-2 | column_order/NA-policy должны ссылаться на единый источник | IO System §Format Layer, Normalization §precision | Централизованная schema registry | Проверка schema hash в meta.yaml | [ref: repo:docs/requirements/02-io-system.md@test_refactoring_32] [ref: repo:docs/requirements/04-normalization-validation.md@test_refactoring_32] |
| AUD-3 | Pandera OutputSchema обязателен для Testitem/Target/Document | Testitem §3-5, Target §1.4, Document §5 | Обновлённые схемы | Pandera validation reports | [ref: repo:docs/requirements/07a-testitem-extraction.md@test_refactoring_32] [ref: repo:docs/requirements/08-target-data-extraction.md@test_refactoring_32] [ref: repo:docs/requirements/09-document-chembl-extraction.md@test_refactoring_32] |
| AUD-4 | CLI-поведение стандартизировано и описано в конфигурации | Assay §7, Configuration §§3-5 | CLI спецификация | pytest CLI contract tests | [ref: repo:docs/requirements/05-assay-extraction.md@test_refactoring_32] [ref: repo:docs/requirements/10-configuration.md@test_refactoring_32] |
| AUD-5 | Логи и QC пороги имеют обязательные поля и AC | Logging §1-3, Assay §4, Activity §11 | Acceptance Criteria | Статические проверки конфигов QC | [ref: repo:docs/requirements/01-logging-system.md@test_refactoring_32] [ref: repo:docs/requirements/05-assay-extraction.md@test_refactoring_32] [ref: repo:docs/requirements/06-activity-data-extraction.md@test_refactoring_32] [ref: repo:docs/gaps.md@test_refactoring_32] |

