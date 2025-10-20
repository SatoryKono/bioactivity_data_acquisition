## Extraction Contract

Основано на `configs/config.yaml`, `configs/config_target_full.yaml`, и загрузчиках конфигов (`library.documents.config.load_document_config`, `library.config.Config`).

### Параметры источников
- Пагинация: `sources.*.pagination.page_param|size_param|size|max_pages`
- Ретраи: `http.global.retries`, `sources.*.http.retries`
- Таймауты: `http.global.timeout_sec`, `sources.*.http.timeout_sec`
- Rate limit: `sources.*.rate_limit.max_calls|period`

### Секреты и заголовки
- Плейсхолдеры `{NAME}` в заголовках → значения из переменных окружения (см. `library.documents.pipeline._create_api_client`).
- Префикс переопределений через окружение: `BIOACTIVITY__...` (см. `DEFAULT_ENV_PREFIX`).

### Фиксация версий/релизов
- ChEMBL release извлекается через статус-клиент (`get_chembl_status`) и сохраняется в метаданных/колонках.
- Версии Python пакетов — через `pyproject.toml` (bounds), публикация в CI.

### Кэширование HTTP
- В коде явный кэш HTTP не реализован; отметить «не реализовано». Возможна реализация на уровне клиентов при необходимости.

### CLI переопределения
- Примеры `--set`: `--set runtime.workers=8`, `--set http.global.timeout_sec=60`.
- Примеры env: `BIOACTIVITY__SOURCES__CROSSREF__ENABLED=false`.

