Аннотация: Пробелы покрытия тестами и требуемые виды тестов. Ветка: test_refactoring_32. Дата: 2025-10-31.

- Унификация UniProt сервисов (Batch 2)
  - Нужны: интеграционные тесты enrichment на фиксированных фикстурах; golden‑сеты результата; property‑based на сопоставление accession→entry.
  - Ссылки: [ref: repo:src/bioetl/sources/uniprot/normalizer_service.py@test_refactoring_32], [ref: repo:src/bioetl/sources/uniprot/merge/service.py@test_refactoring_32]

- Централизация сетевых вызовов ChEMBL status (Batch 3)
  - Нужны: контракт‑тесты таймаутов, 429/Retry‑After, каппинг ожиданий; мок с детерминированными заголовками.
  - Ссылки: [ref: repo:src/bioetl/utils/chembl.py@test_refactoring_32]

- Атомарный I/O отчётов и кэша (Batch 4)
  - Нужны: golden‑тесты Markdown/JSON отчётов (каноническая сериализация, стабильный порядок); тесты восстановления после сбоя кэша.
  - Ссылки: [ref: repo:src/scripts/run_inventory.py@test_refactoring_32], [ref: repo:src/bioetl/utils/validation.py@test_refactoring_32], [ref: repo:src/bioetl/sources/chembl/activity/client/activity_client.py@test_refactoring_32]

- Унификация пагинации (Batch 5)
  - Нужны: property‑based тесты генераторов токенов/offset/page; регресс‑набор с крайними параметрами.
  - Ссылки: [ref: repo:src/bioetl/core/pagination/strategy.py@test_refactoring_32], [ref: repo:src/bioetl/sources/*/pagination/__init__.py@test_refactoring_32]

- Единые билдеры запросов (Batch 6)
  - Нужны: юнит‑тесты формирования заголовков (User‑Agent+mailto/email), детерминированный порядок query.
  - Ссылки: [ref: repo:src/bioetl/sources/openalex/request/builder.py@test_refactoring_32], [ref: repo:src/bioetl/sources/crossref/request/builder.py@test_refactoring_32], [ref: repo:src/bioetl/sources/pubmed/request/builder.py@test_refactoring_32], [ref: repo:src/bioetl/sources/common/request.py@test_refactoring_32]


