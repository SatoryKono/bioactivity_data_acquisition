# Changelog

## [Unreleased]

### Changed

- Синхронизированы схемы assay, activity, testitem с
  IO_SCHEMAS_AND_DIAGRAMS.md
- Удалены QC-поля из activity входа/выхода
- Добавлены обязательные hash поля (hash_row, hash_business_key, index) во все
  пайплайны
- Обновлены Pandera схемы с regex валидацией для ChEMBL ID и hash полей
- Обновлен BaseSchema: добавлена regex валидация для hash полей, исправлены
  типы системных полей
- Обновлена AssaySchema: заменено поле pref_name на description, удалены лишние
  поля, добавлена regex валидация
- Обновлена ActivitySchema: добавлена regex валидация для всех ChEMBL ID полей
- Обновлена TestItemSchema: добавлена regex валидация, обновлен column_order
- Очищены входные CSV файлы (activity.csv, testitem.csv) - оставлены только
  IO_SCHEMAS Input Schema поля
- Добавлена фильтрация входных данных в пайплайнах для исключения QC-полей
- Удалена зависимость `tenacity` из runtime-окружения
- Исправлено количество попыток в fallback-стратегии `partial_retry`

### Added

- Создан `docs/SCHEMA_SYNC_COMPLETION_REPORT.md` с деталями синхронизации схем
