# Changelog

Все заметные изменения проекта фиксируются в этом файле.

## [Unreleased]

### Changed

- Стандартизированы правила логирования и уровни логов
- Обновлены схемы Pandera для всех итоговых таблиц
- Уточнены контракты UnifiedAPIClient (таймауты, ретраи, QPS)
- Удалён дублирующий модуль `src/bioetl/utils/dtype.py`; все импорты

  унифицированы на `bioetl.utils.dtypes`

- Объединены модули `column_validator.py` и `validation.py` в единый

  `validation.py`

- Опция CLI `--limit` снова видна в справке и помечена как устаревшая в пользу

  `--sample`

### Fixed

- Исправлено количество попыток в fallback-стратегии `partial_retry`

### Added

- Создан `docs/SCHEMA_SYNC_COMPLETION_REPORT.md` с деталями синхронизации схем
- Обновлён `meta.yaml`: фиксируются количественные метрики (`quantitative_metrics`),
  длительности стадий в миллисекундах (`stage_durations_ms`), сортировочные ключи
  (`sort_keys` как словарь), `config_version` и политика PII/секретов
- Добавлен `docs/RISK_REGISTER.md` с мониторингом API дрифтов, курсоров, нестабильных
  единиц и отказов записи; обновлены критерии приёмки
