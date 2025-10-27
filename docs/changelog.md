# Релизы и изменения

Следуем SemVer. Формат — Keep a Changelog.

## Процесс выпуска

1. Обновить `CHANGELOG` (секция Unreleased → новый тег)
2. Обновить версии в проекте (при необходимости)
3. Создать тег `vX.Y.Z` и GitHub Release
4. Проверить сборку документации и CI

## [Unreleased] - Documentation Restructuring

### Added

- **Полная реструктуризация документации**: Реализован план актуализации всей документации проекта
- **Новые разделы документации**: 
  - `explanations/data-flow.md` — детальное описание потока данных
  - `explanations/determinism.md` — принципы детерминированности
  - `reference/data-schemas/bioactivity.md` — схемы биоактивности
  - `reference/data-schemas/documents.md` — схемы документов
  - `reference/configuration/schema.md` — детальная схема конфигурации
  - `reference/configuration/examples.md` — примеры конфигураций
  - `reference/outputs/csv-format.md` — спецификация CSV формата
  - `reference/outputs/qc-reports.md` — структура QC отчетов
  - `reference/outputs/correlation-reports.md` — корреляционные отчеты
  - `pipelines/activities.md` — документация пайплайна активностей
  - `pipelines/testitems.md` — документация пайплайна молекул
- **Автогенерация API-документации**: Настроена через mkdocstrings для clients, etl, schemas, config
- **Архивная структура**: Создана `docs/archive/internal-reports/2024-2025/` для исторических документов
- **Документ миграции**: Создан `DOCUMENTATION_MIGRATION.md` с полной картой изменений

### Changed

- **Навигация mkdocs.yml**: Полностью обновлена структура навигации по Diátaxis framework
- **Главная страница**: Добавлена секция пайплайнов и обновлены возможности
- **API документация**: Переработана с автогенерацией из docstrings
- **Структура explanations**: Исправлены битые ссылки и добавлены новые страницы

### Removed

- **Устаревшие файлы**: Удалены дубликаты и устаревшие документы
  - `docs/getting-started.md` (заменен на `tutorials/quickstart.md`)
  - `docs/health-checking.md` (устарел)
  - `docs/validation.md` (интегрирован в `reference/data-schemas/`)
  - `docs/data_schema/` (интегрирован в `reference/data-schemas/`)
  - `docs/data_qc/` (интегрирован в `reference/outputs/`)

### Archived

- **Внутренние отчеты**: Перемещены в архив (~22 файла)
  - Отчеты о реализации и аудиты
  - Планы и чек-листы
  - Устаревшие README и протоколы
  - Папки debug, refactor, migration

### Fixed

- **Битые ссылки**: Исправлены все внутренние ссылки в документации
- **Навигация**: Обновлена структура навигации в mkdocs.yml
- **Конфигурация mkdocs**: Исправлены пути к документации
- **НОВЫЙ**: Разделение слоев: clients/ transforms/ normalize/ postprocessing/
- **НОВЫЙ**: Тесты детерминизма с проверкой SHA256
- **НОВЫЙ**: Адаптер обратной совместимости для постепенной миграции
- **НОВЫЙ**: Миграционный гайд с примерами до/после

### Изменено
- **BREAKING**: ChEMBL client переписан для детерминизма и производительности
- **BREAKING**: Rate limiting на token bucket вместо sliding window
- **BREAKING**: Упрощена архитектура retry (убран circuit breaker / fallback manager)
- **BREAKING**: Конфигурация через Pydantic модели вместо кастомных классов
- **BREAKING**: API клиентов: `_request()` → `fetch()` для ChEMBL
- **BREAKING**: Pipeline интерфейс: `iter_target_batches()` → `fetch_targets()`

### Удалено
- **BREAKING**: circuit_breaker.py (перемещен в library/legacy/)
- **BREAKING**: fallback.py (заменен HTTP-level fallback)
- **BREAKING**: graceful_degradation.py (упрощен до явной обработки в pipeline)
- **BREAKING**: cache_manager.py (заменен TTLCache напрямую в клиентах)

### Исправлено
- **КРИТИЧНО**: Недетерминизм в retry логике (jitter без seed)
- **КРИТИЧНО**: Race conditions в multi-threaded fetch
- **КРИТИЧНО**: DNS fallback создавал недетерминистические результаты
- **КРИТИЧНО**: Dynamic timeout adjustment нарушал воспроизводимость
- **КРИТИЧНО**: Недетерминистический порядок dict keys в JSON сериализации

### Техническая документация
- Полная актуализация документации
- Новая структура навигации по принципам Diátaxis
- FAQ с частыми вопросами
- Документация источников данных и лимитов API
- Автоматические проверки ссылок в CI/CD
- Реорганизована структура docs/ для лучшей навигации
- Обновлены примеры команд под v2 конфигурации
- Улучшена документация CLI и API
- Устранены все битые ссылки
- Удалены дублирующиеся файлы
- Исправлены ссылки на несуществующие файлы

- **Очистка репозитория**: Удалены кэши, временные файлы и большие данные (экономия ~150MB)
- **Обновлён .gitignore**: OS/IDE файлы, test outputs, site/, временные файлы, кэши
- **Обновлены пути конфигурации**: Переход на `configs/`
- **Политики артефактов**: Установлены строгие правила для предотвращения коммита сгенерированных файлов

### Fixed

- **Производительность Git**: Значительно ускорены операции клонирования и работы с репозиторием
- **Управление большими файлами**: Автоматическое отслеживание через Git LFS

### Documentation

- **Новые документы**:
  - [`CLEANUP_REPORT.md`](../../CLEANUP_REPORT.md) - Полный отчёт об очистке репозитория
  - [`docs/how-to/contribute.md`](how-to/contribute.md) - Расширенное руководство по контрибьюшену
  - [`docs/GIT_LFS_WORKFLOW.md`](GIT_LFS_WORKFLOW.md) - Рабочий процесс Git LFS
- **Обновлённые документы**:
  - [`docs/changelog.md`](changelog.md) - Этот файл с записями о релизе очистки

Шаблон (Keep a Changelog):

```markdown
## [Unreleased]
### Added
### Changed
### Fixed

## [0.1.0] - 2025-10-16
### Added
- Initial release
```

## [Unreleased]

### Added
- Р Р°СЃС€РёСЂРµРЅ РїР°СЂСЃРµСЂ Р°СЃСЃРµРµРІ РґР»СЏ РёР·РІР»РµС‡РµРЅРёСЏ РІСЃРµС… 56 РїРѕР»РµР№
- РћР±РЅРѕРІР»РµРЅР° Pandera-СЃС…РµРјР° РґР»СЏ РІР°Р»РёРґР°С†РёРё РЅРѕРІС‹С… РїРѕР»РµР№

