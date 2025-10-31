Аннотация: Нарушения архитектурных правил с привязкой к строкам и минимальными фиксам. Ветка: test_refactoring_32. Дата: 2025-10-31.

| rule | file:line | snippet | impact | fix | required_tests | severity |
|---|---|---|---|---|---|---|
| Сетевые I/O вне client/ | [ref: repo:src/bioetl/utils/chembl.py@test_refactoring_32]:102-109 | `response = requests.get(full_url, timeout=30)` | Обход `UnifiedAPIClient`: несогласованные таймауты/ретраи/троттлинг | Перенести вызов в `core/api_client` и вызывать через `UnifiedAPIClient.request_json` | Контрактные тесты таймаутов/429 Retry-After | High |
| Диск I/O вне output/ и без атомарности | [ref: repo:src/scripts/run_inventory.py@test_refactoring_32]:41-44 | `path.write_text(content, encoding="utf-8")` | Риск недетерминизма и частичных записей | Использовать атомарную запись (temp→fsync→rename) утилитой из `core/output_writer` или локальный helper | Golden-тесты стабильности вывода | Medium |
| Диск I/O вне output/ и без атомарности | [ref: repo:src/bioetl/utils/validation.py@test_refactoring_32]:330-337 | `with open(json_path, "w", ...)` | Возможна потеря отчёта при сбое, нет атомарности | Обернуть запись через атомарный writer,фиксированный порядок ключей | Тест на каноническую сериализацию JSON | Medium |
| Диск I/O вне output/ и без атомарности | [ref: repo:src/bioetl/utils/validation.py@test_refactoring_32]:375-379 | `with open(output_path, "w", ...)` | Нестабильная запись Markdown отчёта | Атомарная запись и фиксированный trailing newline | Golden‑тест MD | Low |
| Диск I/O вне output/ (кэш) | [ref: repo:src/bioetl/sources/chembl/activity/client/activity_client.py@test_refactoring_32]:277-279 | `with cache_path.open("r", ...)` | Несогласованный I/O кэша в слое source | Выделить `CacheIO` в core/output и применять атомарность | Тесты на целостность кэша | Medium |
| Диск I/O вне output/ (кэш) | [ref: repo:src/bioetl/sources/chembl/activity/client/activity_client.py@test_refactoring_32]:366-367 | `with cache_path.open("w", ...)` | Возможна порча кэша при сбое | Атомарная запись JSON, сортировка ключей | Тест на восстановление после сбоя | Medium |
| Дубли ролей пагинации (пересечение слоёв) | [ref: repo:src/bioetl/core/pagination/strategy.py@test_refactoring_32], [ref: repo:src/bioetl/sources/*/pagination/__init__.py@test_refactoring_32] | классы `Cursor/Offset/PageNumber*` в двух местах | Расходимость контрактов пагинации | Свести реализацию в core; в sources оставить thin wrappers | Контракт‑тесты пагинации на наборах входов | Medium |
| Дубли клиентов UniProt | [ref: repo:src/bioetl/sources/uniprot/client.py@test_refactoring_32]:17 | `warn_legacy_client(...)` | Две параллельные реализации клиентов | Вывести монолит из оборота в пользу пакетных `client/*` | Интеграционные тесты enrichment | Medium |
| Потенциальная несогласованность схем одной сущности | [ref: repo:src/bioetl/schemas/document.py@test_refactoring_32], [ref: repo:src/bioetl/schemas/document_input.py@test_refactoring_32] | разные определения `document*` | Риски расхождения колонок/input→output | Выровнять nullable/column_order, зафиксировать Pandera‑политики | Pandera‑тесты на эквивалентность маппинга | Low |

Минимальные корректировки дизайна (общее)
- Сетевые вызовы централизовать в `core/api_client` с Retry‑After, backoff, бюджетом попыток.
- Любой файловый I/O — через атомарные писатели `core/output_writer` (включая кэш и отчёты).
- Пагинацию унифицировать в core, источникам оставить только специфику query.
- Удалить устаревшие клиенты UniProt, поддержать shim до миграции.


