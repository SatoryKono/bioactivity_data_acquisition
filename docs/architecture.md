# Архитектура и поток данных

Следующая диаграмма показывает общий поток ETL и обработку ошибок/лимитов.

```mermaid
flowchart LR
  A[Источники API (ChEMBL, Crossref, PubMed...)] --> B[HTTP клиенты (requests + backoff)]
  B --> C[ETL: извлечение/нормализация]
  C --> D[Pandera валидация]
  D --> E[Экспорт CSV (детерминированный)]
  E --> F[QC отчёты]
  E --> G[Корреляции]
  B --> H{Ошибки/лимиты}
  H -->|retry/backoff| B
  H -->|логирование| I[Логи/метрики]
```

## Потоки ошибок и ретраи
```mermaid
sequenceDiagram
  participant Client as HTTP клиент
  participant API as Внешний API
  Client->>API: GET /endpoint
  API-->>Client: 429 Too Many Requests
  Client->>Client: backoff (экспоненциально)
  Client->>API: повтор запроса
  API-->>Client: 200 OK | 5xx | 4xx
  Client->>Client: логирование, анализ заголовка Retry-After
```

## Точки расширения
- Клиенты API в `src/library/clients`
- Трансформации/ETL в `src/library/etl`
- Схемы данных в `src/library/schemas`

## Потоки ошибок
- 4xx/5xx → экспоненциальный backoff, ограничение попыток
- 429 (rate limit) → пауза/джиттер, повтор; чтение `Retry-After`
- Фолы валидации Pandera → отчёты/логи, корректирующие действия
