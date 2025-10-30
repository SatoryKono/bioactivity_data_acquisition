# 99. Итоговая техническая спецификация

## Назначение документа
Итоговая спецификация консолидирует ключевые требования к архитектуре, конфигурации, CLI, логированию, HTTP-клиентам, нормализации, системе вывода данных, нефункциональным характеристикам и стратегиям QA для унифицированного ETL-пайплайна. Документ агрегирует положения из профильных стандартов `00-10`, расширяющих требований и аудиторских материалов, чтобы служить единым входом для дальнейшей разработки и приёмки.【F:docs/requirements/00-architecture-overview.md†L3-L33】【F:docs/requirements/10-configuration.md†L1-L173】【F:docs/REQUIREMENTS_AUDIT.md†L1-L51】

## Архитектура и модульная композиция
- **Архитектурные принципы**: композиция над наследованием, детерминизм, безопасность и расширяемость определяют построение всех подсистем (UnifiedLogger, UnifiedOutputWriter, UnifiedAPIClient, UnifiedSchema).【F:docs/requirements/00-architecture-overview.md†L13-L164】
- **Компонентное взаимодействие**: пайплайн использует последовательную цепочку логирования → HTTP-клиенты → нормализаторы/схемы → детерминированный вывод, что фиксируется диаграммой взаимодействия и соответствует унифицированным требованиям каждого компонента.【F:docs/requirements/00-architecture-overview.md†L33-L199】

## Конфигурация и CLI
- **Стандарт конфигурации**: YAML + Pydantic модель `PipelineConfig`, поддержка наследования профилей, правила приоритета переопределений, alias/ENV-binding и валидация версии.【F:docs/requirements/10-configuration.md†L1-L175】【F:docs/requirements/10-configuration.md†L300-L311】
- **CLI-инварианты**: стандартный набор флагов (`--config`, `--golden`, `--sample`, `--fail-on-schema-drift`, `--extended`, `--mode`, `--dry-run`, `--verbose`) и механизм `--set`/`BIOETL_*` обеспечивают единое управление параметрами всех профилей пайплайна.【F:docs/requirements/10-configuration.md†L177-L288】

## Система логирования
- UnifiedLogger обеспечивает структурированное логирование через structlog, обязательные поля контекста (`run_id`, `stage`, `actor`, `source`, `generated_at` и др.), фильтрацию секретов и режимы для разных окружений.【F:docs/requirements/00-architecture-overview.md†L35-L59】【F:docs/requirements/01-logging-system.md†L1-L138】
- Проверка обязательных полей логов закреплена в acceptance criteria (AC11), поддерживая трассируемость и соответствие требованиям аудита (AUD-5).【F:docs/acceptance-criteria.md†L21-L41】【F:docs/REQUIREMENTS_AUDIT.md†L36-L58】

## HTTP-клиенты и устойчивость извлечения
- UnifiedAPIClient реализует слои кэширования, circuit breaker, fallback-стратегии, token-bucket rate limiting с jitter, экспоненциальные повторы и управление пагинацией для REST API источников (ChEMBL, PubMed, CrossRef и др.).【F:docs/requirements/00-architecture-overview.md†L112-L135】【F:docs/requirements/03-data-extraction.md†L1-L195】
- Acceptance критерии охватывают соблюдение Retry-After (AC5) и другие контракты отказоустойчивости, обеспечивая проверяемость устойчивости клиента.【F:docs/acceptance-criteria.md†L7-L20】【F:docs/acceptance-criteria.md†L145-L160】

## Нормализация и валидация
- UnifiedSchema предоставляет модульные нормализаторы (строковые, числовые, химические, идентификаторы, онтологии) и семейство Pandera-схем для входных/выходных таблиц всех сущностей, гарантируя контроль типов и метаданных нормализации.【F:docs/requirements/00-architecture-overview.md†L138-L164】【F:docs/requirements/04-normalization-validation.md†L1-L200】
- Требования аудита (AUD-2, AUD-3) фиксируют необходимость централизованного column_order/NA-policy и обязательных Pandera OutputSchema, дополняя acceptance criteria по проверке колонок и дрифтов схем (AC2, AC10).【F:docs/REQUIREMENTS_AUDIT.md†L28-L58】【F:docs/acceptance-criteria.md†L7-L20】【F:docs/acceptance-criteria.md†L114-L143】

## Вывод данных, QC и корреляции
- UnifiedOutputWriter гарантирует атомарную запись через временные каталоги, детерминированные CSV (порядок строк/столбцов, каноническая сериализация), QC отчёты, correlation отчёты по флагу и поддержку расширенных артефактов (meta.yaml, manifest).【F:docs/requirements/00-architecture-overview.md†L61-L110】【F:docs/requirements/02-io-system.md†L1-L144】
- Acceptance критерии AC1, AC3, AC4, AC6 описывают проверки детерминизма, отсутствия частичных артефактов и стабильной сортировки, связывая вывод с QA-практиками.【F:docs/acceptance-criteria.md†L7-L113】

## Нефункциональные требования
- **Детерминизм**: все стадии используют фиксированное время (UTC), сортировки и сериализацию, обеспечивая воспроизводимость и поддерживаемые golden-run проверки.【F:docs/requirements/00-architecture-overview.md†L19-L24】【F:docs/acceptance-criteria.md†L7-L20】
- **Безопасность**: редактирование секретов в логах, управление конфиденциальными параметрами через ENV, fail-fast на несогласованных конфигурациях и защита от частичных записей удовлетворяют базовым требованиям безопасности и целостности.【F:docs/requirements/00-architecture-overview.md†L25-L31】【F:docs/requirements/10-configuration.md†L177-L204】【F:docs/requirements/02-io-system.md†L70-L144】
- **Расширяемость и сопровождение**: модульная архитектура, профили конфигураций и таблица линтеров/типизации создают базу для масштабируемого развития пайплайнов.【F:docs/requirements/00-architecture-overview.md†L29-L33】【F:docs/requirements/10-configuration.md†L300-L311】

## Стратегия QA и контроль качества
- Requirements Audit фиксирует ключевые риски (AUD-1…AUD-5) по лимитам, схемам, CLI и логированию, определяя направления доработок и проверки полноты требований.【F:docs/REQUIREMENTS_AUDIT.md†L23-L58】
- Acceptance criteria охватывают golden-run, schema drift, QC-пороги для всех пайплайнов и обязательные поля логов, обеспечивая проверяемые контракты приёмки.【F:docs/acceptance-criteria.md†L7-L155】
- Документы по статусу/отчётности (например, `DOCUMENT_PIPELINE_VERIFICATION.md`, `SCHEMA_COMPLIANCE_REPORT.md`) остаются источниками доказательств выполнения AC и устранения аудиторских замечаний.

## Матрица трассируемости
| ID | Требование | Источник | Метод проверки | QA-артефакт |
|---|---|---|---|---|
| FTS-ARCH-1 | Архитектура пайплайна следует принципам композиции, детерминизма и расширяемости | 00-architecture-overview.md §§Принципы, Компоненты | Архитектурный обзор + code review модульных реализаций | REQUIREMENTS_AUDIT.md (инвентаризация архитектуры) |
| FTS-CONF-1 | Конфигурация и CLI стандартизированы (PipelineConfig, единые флаги, overrides) | 10-configuration.md §§1-6 | pytest CLI contract tests, проверка `PipelineConfig.validate_yaml` | acceptance-criteria.md (AC10), REQUIREMENTS_AUDIT.md (AUD-4) |
| FTS-LOG-1 | Логи содержат обязательные поля и защищают секреты | 00-architecture-overview.md §UnifiedLogger; 01-logging-system.md §§Компоненты | pytest log field test (AC11), статическая проверка фильтров | acceptance-criteria.md (AC11), REQUIREMENTS_AUDIT.md (AUD-5) |
| FTS-HTTP-1 | HTTP-клиент поддерживает retries, rate limiting, fallback и circuit breaker | 00-architecture-overview.md §UnifiedAPIClient; 03-data-extraction.md §§Архитектура, Компоненты | Интеграционные тесты с mock API, проверка AC5 | acceptance-criteria.md (AC5), REQUIREMENTS_AUDIT.md (AUD-1) |
| FTS-NORM-1 | Нормализация использует Pandera OutputSchema и реестр нормализаторов | 00-architecture-overview.md §UnifiedSchema; 04-normalization-validation.md §§Архитектура | Pandera validation (AC2), schema drift tests (AC10) | acceptance-criteria.md (AC2/AC10), REQUIREMENTS_AUDIT.md (AUD-2/AUD-3) |
| FTS-IO-1 | Вывод детерминирован и атомарен, QC/корреляции формируются по конфигурации | 00-architecture-overview.md §UnifiedOutputWriter; 02-io-system.md §§Компоненты | Golden-run сравнения, проверка атомарной записи и QC порогов | acceptance-criteria.md (AC1/AC3/AC4/AC12-AC16) |
| FTS-QA-1 | QA покрывает лимиты, QC пороги и устранение аудиторских рисков | REQUIREMENTS_AUDIT.md §§Диагностика, Матрица; acceptance-criteria.md | Выполнение чек-листа приёмки, аудит отчётов | REQUIREMENTS_AUDIT.md (AUD-1…AUD-5), FINAL_VALIDATION_REPORT.md |

## Чек-лист приёмки
1. **Конфигурация**: профили наследуют `base.yaml`, проходят `PipelineConfig.validate_yaml`, CLI флаги соответствуют таблице и приоритетам переопределений (AUD-4).【F:docs/requirements/10-configuration.md†L1-L311】【F:docs/REQUIREMENTS_AUDIT.md†L36-L58】
2. **Логирование**: логи включают обязательный контекст, секреты редактируются, AC11 выполняется для всех пайплайнов (AUD-5).【F:docs/requirements/01-logging-system.md†L1-L138】【F:docs/acceptance-criteria.md†L21-L41】【F:docs/REQUIREMENTS_AUDIT.md†L36-L58】
3. **HTTP-устойчивость**: клиенты выдерживают Retry-After, rate limiting и circuit breaker сценарии, AC5 проходит на интеграционных тестах (AUD-1).【F:docs/requirements/03-data-extraction.md†L1-L195】【F:docs/acceptance-criteria.md†L145-L160】【F:docs/REQUIREMENTS_AUDIT.md†L23-L58】
4. **Нормализация/схемы**: Pandera OutputSchema актуализированы для всех пайплайнов, column_order/NA-policy ссылочно едины (AUD-2/AUD-3), AC2/AC10 выполняются.【F:docs/requirements/04-normalization-validation.md†L1-L200】【F:docs/acceptance-criteria.md†L7-L20】【F:docs/REQUIREMENTS_AUDIT.md†L28-L58】
5. **Вывод и QC**: детерминированные CSV/отчёты формируются атомарно, QC пороги (AC12-AC16) выполняются, отсутствуют частичные артефакты (AC1/AC3/AC4/AC6).【F:docs/requirements/02-io-system.md†L1-L144】【F:docs/acceptance-criteria.md†L7-L155】
6. **Документация и отчёты**: актуализированы артефакты валидации (`DOCUMENT_PIPELINE_VERIFICATION.md`, `SCHEMA_COMPLIANCE_REPORT.md`, `FINAL_VALIDATION_REPORT.md`) с ссылками на устранение рисков из аудита.【F:docs/REQUIREMENTS_AUDIT.md†L23-L58】

## Ссылочная карта документов
- `00-architecture-overview.md` — архитектура, компоненты, инварианты детерминизма.
- `01-logging-system.md` — профиль UnifiedLogger и обязательные поля контекста.
- `02-io-system.md` — атомарная запись, QC, correlation и метаданные вывода.
- `03-data-extraction.md` — устойчивые HTTP-клиенты, rate limiting, retries.
- `04-normalization-validation.md` — нормализаторы и Pandera-схемы.
- `05-09` — профильные пайплайны (assay, activity, testitem, target, document).
- `10-configuration.md` — стандарты конфигурации и CLI.
- `REQUIREMENTS_AUDIT.md` — выявленные риски, аудит и матрица AUD.
- `acceptance-criteria.md` — проверяемые AC и тестовые методики.
- Профильные отчёты в `docs/` — подтверждение выполнения требований и QA.
