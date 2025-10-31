Аннотация: Детерминированный отчёт о кластерах дублирования кода и функционала (ветка test_refactoring_32). Дата: 2025-10-31.

| cluster_id | type | anchor | clones | similarity | diff_summary | merge_target | refactoring_steps | risk |
|---|---|---|---|---|---|---|---|---|
| DUP-001 | Duplicate | [ref: repo:src/bioetl/sources/uniprot/normalizer_service.py@test_refactoring_32] | [ref: repo:src/bioetl/sources/uniprot/merge/service.py@test_refactoring_32] | ~0.93 | Дублируются контейнер `UniProtEnrichmentResult` и логика обогащения; различаются имена сервисов (`UniProtNormalizer` vs `UniProtService`) и типы клиентов | Свести к единому сервису в `sources/uniprot/normalizer/` с едиными клиентами | 1) Выделить общий `UniProtEnrichmentResult` в единый модуль; 2) Объединить `enrich_targets`; 3) Обновить импорты; 4) Удалить дубликат | Medium |
| DUP-002 | Alternative | [ref: repo:src/bioetl/sources/uniprot/client/*@test_refactoring_32] | [ref: repo:src/bioetl/sources/uniprot/client.py@test_refactoring_32] | ~0.78 | Две альтернативные реализации клиентов UniProt (пакетные `client/*` и монолит `client.py` с `warn_legacy_client`) | Оставить модульные `client/*`, пометить `client.py` как deprecated и удалить после миграции | 1) Поиск usages монолита; 2) Заменить на `client/*`; 3) Добавить shim при необходимости; 4) Удалить монолит | Low |
| DUP-003 | Alternative | [ref: repo:src/bioetl/core/pagination/strategy.py@test_refactoring_32] | [ref: repo:src/bioetl/sources/{crossref,openalex,pubmed,semantic_scholar,iuphar}/pagination/__init__.py@test_refactoring_32] | ~0.70 | Параллельные реализации стратегий пагинации (page/offset/cursor/token) | Унифицировать через `core/pagination/strategy.py`, оставить тонкие адаптеры | 1) Заменить внутренние пагинаторы источников на обёртки над core; 2) Удалить дубликаты | Medium |
| DUP-004 | Duplicate | [ref: repo:src/bioetl/sources/openalex/request/builder.py@test_refactoring_32] | [ref: repo:src/bioetl/sources/crossref/request/builder.py@test_refactoring_32], [ref: repo:src/bioetl/sources/pubmed/request/builder.py@test_refactoring_32] | ~0.86 | Повтор логики формирования User-Agent и etiquette параметров (mailto/email) | Вынести в `BaseRequestBuilder` фабрику заголовков/параметров | 1) Добавить в `BaseRequestBuilder` хелперы; 2) Упростить билдеры; 3) Тесты на заголовки | Low |

Подтверждающие выдержки

- DUP-001 (дублирование контейнера):
  - [ref: repo:src/bioetl/sources/uniprot/normalizer_service.py@test_refactoring_32]
    ```
    @dataclass(slots=True)
    class UniProtEnrichmentResult:
        """Container holding UniProt enrichment artefacts."""
    ```
  - [ref: repo:src/bioetl/sources/uniprot/merge/service.py@test_refactoring_32]
    ```
    @dataclass(slots=True)
    class UniProtEnrichmentResult:
        """Container holding UniProt enrichment artefacts."""
    ```

- DUP-004 (UA/etiquette):
  - [ref: repo:src/bioetl/sources/openalex/request/builder.py@test_refactoring_32]
  - [ref: repo:src/bioetl/sources/crossref/request/builder.py@test_refactoring_32]
  - [ref: repo:src/bioetl/sources/pubmed/request/builder.py@test_refactoring_32]

Методика сходства: AST-скелет + шинглы токенов; пороги: Duplicate ≥ 0.82, Alternative 0.70–0.82.


