# Архив устаревших файлов документации

## Обзор

Этот каталог содержит файлы документации, которые были признаны устаревшими в ходе рефакторинга документации в октябре 2025 года.

## Причины архивации

### Дублирующиеся файлы

- `docs/reference/data_normalization.md` - дублирует `docs/reference/data-normalization.md`
- `docs/reference/data_schema.md` - устаревшая схема, заменена на актуальные Pandera схемы

### Устаревшие разделы

- `docs/setup/` - заменены на `docs/how-to/installation.md` и `docs/how-to/configure-api-clients.md`
- `docs/guides/` - заменены на `docs/how-to/` разделы

### Отчёты рефакторинга

- `docs/refactoring/` - исторические отчёты о рефакторинге, не относящиеся к пользовательской документации

## Миграция

### setup/ → how-to/

- `setup/install.md` → `how-to/installation.md`
- `setup/config.md` → `how-to/configure-api-clients.md`

### guides/ → how-to/

- `guides/howto_add_pipeline.md` → `how-to/development.md`
- `guides/troubleshooting.md` → `how-to/debug-pipeline.md`

### reference/ дубликаты

- `reference/data_normalization.md` → удалён (дубликат)
- `reference/data_schema.md` → удалён (устаревшая схема)

## Дата архивации

2025-10-24

## Ответственный

Data Engineering Team
