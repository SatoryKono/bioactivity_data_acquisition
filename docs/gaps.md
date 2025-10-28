# Gap-лист проекта

Документ содержит идентифицированные пробелы в текущей реализации и требованиях проекта.

## Таблица пробелов

| id | категория | описание пробела | влияние/риск | как воспроизвести | приоритет | ref | предложенное исправление | владелец | ETA |
|---|---|---|---|---|---|---|---|---|---|
| G1 | IO/Детерминизм | Atomic write описан в нескольких местах, нет единого норматива и AC запрета partial | Частичные артефакты, коррапт | Прервать запись при ошибке | High | [02-io-system.md → Протокол Atomic Write](requirements/02-io-system.md#протокол-atomic-write) | Вынести единый раздел и сослаться из всех пайплайнов; добавить AC | Архитектор | 2д |
| G2 | Клиент API | ~~Нет предметного запрета смешивать pagination в activity~~ | ~~Потеря страниц~~ | ~~Передать одновременно offset+cursor~~ | ~~High~~ | [03-data-extraction.md → Валидация стратегии](requirements/03-data-extraction.md#запрет-смешивания-стратегий) | ✅ **CLOSED** (v3.0: все ChEMBL pipelines унифицированы на batch IDs стратегию) | - | - |
| G3 | Клиент API | «UNCERTAIN limit» без AC и фиксации | Нестабильность выгрузки | Поднять limit до 5000 | Med | [06-activity-data-extraction.md → UNCERTAIN](requirements/06-activity-data-extraction.md#⚠️-uncertain-максимальный-limit) | Добавить бинарный поиск лимита и зафиксировать результат | Data Eng | 2д |
| G4 | Нормализация/Схемы | column_order дублируется в meta.yaml | Дрейф порядка | Сменить порядок в одном месте | High | [04-normalization-validation.md → Хранение column_order](requirements/04-normalization-validation.md#хранение-column_order-в-схеме) | Источник истины — схема; meta.yaml только копия | Архитектор | 1д |
| G5 | Нормализация/Схемы | Нет единых NA-policy/precision в AC | Несовпадающие хеши | Разные округления | Med | [04-normalization-validation.md → precision_map](requirements/04-normalization-validation.md#метрики-precision) | Ввести precision_policy и NA-policy как инварианты | Data Eng | 2д |
| G6 | Assay | Нет явного batch≤25 в 05-файле | 413/URI too long | batch=100 | High | [00-architecture-overview.md → Assay batch](requirements/00-architecture-overview.md#конфигурация) | Добавить требование и валидацию конфига | ETL Eng | 1д |
| G7 | Assay | Нет «long-format или ошибка» для nested | Потеря данных | Не раскрыть variant_sequences | High | [00-architecture-overview.md → Long format](requirements/00-architecture-overview.md) | Ввести expand_* функции и AC | ETL Eng | 2д |

| G8 | Assay | Нет whitelist-enrichment и строгой проверки «лишних полей» | Шум, дрейф | Неподдерж. поля в output | Med | [00-architecture-overview.md → Whitelist enrichment](requirements/00-architecture-overview.md) | --strict-enrichment + schema check | ETL Eng | 2д |

| G9 | Activity | Не зафиксирована сортировка по activity_id в шаге записи | Недетерминизм | Перезапуск пайплайна | High | [06-activity-data-extraction.md → Детерминированная сортировка](requirements/06-activity-data-extraction.md#детерминизм) | Перед записью sort_values(["activity_id"]) | Data Eng | 1д |
| G10 | Activity | QC-фильтры по validity/duplicates не AC | Захламление | Дубликаты в выдаче | Med | [06-activity-data-extraction.md → QC-фильтры](requirements/06-activity-data-extraction.md#11-quality-control) | Ввести AC: duplicates_activity_id==0 | Data Eng | 1д |
| G11 | Клиент API | Respect Retry-After не закреплён как инвариант | Агрессивные ретраи | 429-шторма | High | [03-data-extraction.md → AC-07 Retry-After](requirements/03-data-extraction.md#ac-07-respect-retry-after-429) | В Acceptance включить проверку и метрику | Архитектор | 1д |
| G12 | Логи | Обязательные поля логов не требование для всех модулей | Просадка диагностики | Нет trace/run_id | Med | [01-logging-system.md → Примеры и поля](requirements/01-logging-system.md) | Ввести список обязательных полей | DevOps | 1д |
| G13 | IO/Детерминизм | Отсутствие единой команды golden-run в AC | Незаметный дрейф | Сравнить run-to-run | Med | [02-io-system.md → Golden comparison](requirements/02-io-system.md#сценарий-golden-run) | AC: bit-exact сравнение с golden | QA | 2д |
| G14 | Testitem | Корреляции по умолчанию выключены, но нет явного AC | Ложные алерты | Нестабильные метрики | Low | [00-architecture-overview.md → Корреляции опциональны](requirements/00-architecture-overview.md) | AC: correlation_report опционален | Data Eng | 1д |
| G15 | Testitem | Единый atomic-write есть в 07a, но не сосл. повсеместно | Расхождение практик | Разный cleanup | Med | [07a-testitem-extraction.md → Atomic Writes](requirements/07a-testitem-extraction.md) | Сослать на IO-норматив | Архитектор | 1д |

## Приоритизация

- **High**: G1, G2, G6, G7, G9, G11 (критично для детерминизма и отказоустойчивости)

- **Med**: G3, G4, G5, G8, G10, G12, G13, G15 (важно для качества и поддерживаемости)

- **Low**: G14 (минорное улучшение)

## Связи с AC

| Gap ID | Acceptance Criteria |
|--------|-------------------|
| G1 | AC1, AC4 |
| G2 | - |

| G3 | - |

| G4 | AC2 |
| G5 | AC3 |
| G6 | AC7 |
| G7 | AC8 |
| G8 | AC10 |
| G9 | AC6 |
| G10 | AC9 |
| G11 | AC5 |
| G12 | - |

| G13 | AC1 |
| G14 | - |

| G15 | AC1, AC4 |

## Методология оценки

Gap-лист составлен на основе оценки по ISO/IEC 25010:

- Функциональная полнота и проверяемость

- Надёжность и отказоустойчивость

- Производительность и лимиты

- Безопасность и соблюдение контрактов

- Сопровождаемость и детерминизм

- Совместимость и переносимость

- Наблюдаемость и трассировка

- Пользовательская пригодность CLI

- Архитектурные trade-off'ы

и ATAM (Architecture Trade-off Analysis Method):

- Риски: детерминизм вывода, отказоустойчивость под лимитами API, эволюция схем (semver), целостность ссылок (RI)

- Компромиссы: производительность vs. детерминизм, гибкость vs. строгость схем
