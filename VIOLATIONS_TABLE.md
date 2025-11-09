# VIOLATIONS_TABLE

## Сводка аудита (2025-11-08)

Обновлённый сканер `bioetl.tools.naming_violation_scan` не обнаружил активных нарушений
правил именования в `src/bioetl`. Историческая таблица заменена на краткий отчёт,
чтобы исключить ложные срабатывания и сфокусироваться на реальных проблемах.

| path | category | identifier | rule_id | rationale |
|---|---|---|---|---|

## Классификация ложных срабатываний

- **Dunder-символы** (`__init__`, `__iter__`) и файлы `__init__.py` whitelisted.
- **Приватные snake_case-хелперы** (`_normalize_*`, `_ensure_*`) считаются корректными.
- **Импортные алиасы** (`import pandas as _pd`, `DataFrame as SparkDataFrame`) не трактуются как константы.
- **NodeVisitor-методы** (`visit_Assign`, `visit_ClassDef`) допускают PascalCase после префикса `visit_`.

## Перезапуск проверки

```powershell
$env:PYTHONPATH = 'src'
$env:PYTHONIOENCODING = 'utf-8'
python -m bioetl.tools.naming_violation_scan scan --sources src/bioetl
```

Опция `--output <path>` записывает markdown-таблицу (детерминированный порядок, UTF-8 без BOM).