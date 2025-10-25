# Релизы и изменения

Следуем SemVer. Формат — Keep a Changelog.

## Процесс выпуска

1. Обновить `CHANGELOG` (секция Unreleased → новый тег)
2. Обновить версии в проекте (при необходимости)
3. Создать тег `vX.Y.Z` и GitHub Release
4. Проверить сборку документации и CI

## [Unreleased] - Data Extraction Refactoring

<<<<<<< Updated upstream
### Added

- **Документация очистки репозитория**: Создан `CLEANUP_REPORT.md` с полным описанием проведённой очистки
- **Расширенное руководство по контрибьюшену**: Добавлены детальные инструкции по работе с артефактами, Git LFS и pre-commit хуками
- **Документация (MkDocs Material)**: API Reference, диаграммы, CI публикации
- **Git LFS интеграция**: Настроен для `*.parquet`, `*.pkl`, `*.xlsm`, `*.png`, `*.jpg` и других больших форматов
- **Pre-commit хуки**: Порог 500 КБ, блокировка артефактов, проверка секретов
- **CI артефакты**: Публикация test outputs, coverage и security отчётов

### Changed
=======
### Добавлено
- **НОВЫЙ**: ChEMBL client v2 с детерминистическим поведением
- **НОВЫЙ**: Token bucket rate limiter с adaptive backoff
- **НОВЫЙ**: Pydantic модели конфигурации (ApiCfg, RetryCfg, ChemblCacheCfg)
- **НОВЫЙ**: Детерминистический jitter с фиксированным seed
- **НОВЫЙ**: TTL кэш с thread-safety гарантиями
- **НОВЫЙ**: Streaming поддержка для больших датасетов
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
>>>>>>> Stashed changes

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
