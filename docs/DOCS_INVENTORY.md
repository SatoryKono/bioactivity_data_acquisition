# DOCS INVENTORY (pre-revision)

Каталог: docs/

## Основные разделы (предварительно актуально)
- activity.md, activity_usage.md
- assay.md, assay_usage.md
- README_main.md, README.md
- getting-started.md, tutorials/*
- reference/*
- api/*
- postprocess/documents.md

## Технические/процессные документы (кандидаты на ревизию после кода)
- CLEANUP_REPORT.md — временный отчёт
- LOGGING_AUDIT.md — временный аудит
- LOGGING_CHANGE_PLAN.md — план изменений
- LOGGING_IMPLEMENTATION_REPORT.md — отчёт реализации
- STAGE11_COMPLETION_REPORT.md, STAGE11_README.md — стадийные материалы
- ASSESSMENT.md — оценочный документ
- CASE_SENSITIVITY_FIX_SUMMARY.md — локальный фикс-отчёт
- debug/FIXES_SUMMARY.md — отладочный отчёт
- IMPROVEMENT_PLAN.md, REFORM_PLAN.md — планы
- DOCS_AUDIT.md, DOCS_CHECKLIST.md, CHECKLIST*.md — чек-листы

## Примечание
Окончательные решения об удалении/слиянии будут приняты после завершения кодовой очистки и проверки актуальности ссылок/скриншотов в текстах.

---

# DOCS INVENTORY (refactoring map)

Ниже — целевая структура и маппинг существующих страниц на новую иерархию S00–S08. Переносим устаревшие/дублирующие материалы в `docs/archive/` с пометкой Deprecated.

## Новые разделы и страницы
- Пайплайны (по шаблону S00–S08):
  - `docs/pipelines/documents.md`
  - `docs/pipelines/testitem.md`
  - `docs/pipelines/assay.md`
  - `docs/pipelines/activity.md`
  - `docs/pipelines/target.md`
- Конфигурация источников: `docs/configuration/sources.md`
- Контракт извлечения: `docs/extraction-contract.md`
- Нормализация: `docs/normalization.md`
- Обогащение: `docs/enrichment.md`
- Валидация: `docs/validation.md`
- Качество (QC): `docs/data_qc/README.md`
- Персистентность и детерминизм: `docs/persistence.md`
- CLI обзор: `docs/cli.md`
- Логи и метрики: `docs/logging.md`
- Политика тестов: `docs/tests.md`
- Star-Schema интеграция: `docs/star-schema.md`
- Границы/исключения: `docs/non-goals.md`
- Выходные артефакты: `docs/output-artifacts.md`

## Источники фактов (линки в новых страницах)
- CLI: `src/library/cli/__init__.py`, `src/library/cli/__main__.py`
- Конфиги: `configs/config.yaml`, `configs/config_target_full.yaml`, `reports/config_audit.csv`
- Логирование/детерминизм: `src/library/logging_setup.py`
- Пайплайны: `src/library/assay/pipeline.py`, `src/library/pipelines/target/iuphar_target.py`, `src/library/pipelines/target/postprocessing.py`, (и аналоги для documents/testitem/activity)
- Артефакты: `MANIFEST.json`, `POSTPROCESS_MANIFEST.json`, `data/output/**`

## Архивация (перенос в docs/archive/)
К переносу с шильдиком «Deprecated»:
- `docs/CLEANUP_REPORT.md`
- `docs/LOGGING_AUDIT.md`
- `docs/LOGGING_CHANGE_PLAN.md`
- `docs/LOGGING_IMPLEMENTATION_REPORT.md`
- `docs/STAGE11_COMPLETION_REPORT.md`
- `docs/STAGE11_README.md`
- `docs/ASSESSMENT.md`
- `docs/CASE_SENSITIVITY_FIX_SUMMARY.md`
- `docs/debug/FIXES_SUMMARY.md`
- `docs/IMPROVEMENT_PLAN.md`
- `docs/REFORM_PLAN.md`
- `docs/DOCS_AUDIT.md`
- `docs/DOCS_CHECKLIST.md`
- `docs/CHECKLIST_postprocess_documents.md`
- `docs/CHECKLIST.md`
- `docs/TEST_OUTPUTS_CLEANUP.md`
- (устар.) `docs/README_LOGGING_SECTION.md`

## Маппинг существующих страниц
- (объединено) `docs/activity.md`, `docs/activity_usage.md` → `docs/pipelines/activity.md`
- (объединено) `docs/assay.md`, `docs/assay_usage.md` → `docs/pipelines/assay.md`
- `docs/postprocess/documents.md`, `docs/reference/data-schemas/*`, `docs/data_qc/*` → ссылки в `docs/pipelines/documents.md` и `docs/data_qc/README.md`

### Статус выполнения (housekeeping)
- Все дубликаты удалены из корня `docs/`
- Пайплайны сведены в `docs/pipelines/*`
- Логирование объединено в `docs/logging.md`
- Схемы сведены в `docs/reference/data-schemas/index.md`
- QC канонизирован в `docs/data_qc/README.md`
- Навигация `configs/mkdocs.yml` обновлена
- `docs/api-limits.md`, `docs/health-checking.md` → ссылки в `docs/extraction-contract.md`
- `docs/README_main.md` объединён в `docs/index.md`; `docs/README.md` оставить при отличиях; `docs/getting-started.md` остаётся

## Навигация MkDocs
Навигацию обновить после создания новых страниц: добавить раздел «Пайплайны», «Configuration → Sources», «Persistence & Determinism», «Logging & Metrics», «Tests», «Output Artifacts». Не ссылаться на несуществующие файлы (strict: true).