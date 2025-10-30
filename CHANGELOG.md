# Changelog
Все заметные изменения проекта фиксируются в этом файле.

## [Unreleased]
### Changed
- Стандартизированы правила логирования и уровни логов
- Обновлены схемы Pandera для всех итоговых таблиц
- Уточнены контракты UnifiedAPIClient (таймауты, ретраи, QPS)
- Удалён дублирующий модуль `src/bioetl/utils/dtype.py`; все импорты унифицированы на `bioetl.utils.dtypes`

### Fixed
- Исправлено количество попыток в fallback-стратегии `partial_retry`

### Added
- Создан `docs/SCHEMA_SYNC_COMPLETION_REPORT.md` с деталями синхронизации схем
