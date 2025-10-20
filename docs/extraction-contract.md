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

---

## Ограничения API и мониторинг

### Быстрые проверки и лимиты

Ниже приведены вспомогательные команды для быстрых проверок доступности API и их лимитов:

```bash
python -m library.tools.quick_api_check
python -m library.tools.api_health_check --save

# Проверка лимитов
python -m library.tools.check_api_limits
python -m library.tools.check_specific_limits --source chembl
```

Поддерживаемые значения `--source`: `chembl`, `crossref`, `openalex`, `pubmed`, `semantic_scholar`.

### Health checking (стратегии)

Система проверки здоровья опрашивает базовые или специальные эндпоинты:

- BASE_URL (по умолчанию): 200/404/405 трактуются как «здоровый» для внешних API без спец. эндпоинтов
- CUSTOM_ENDPOINT: ожидаем 2xx; любые 4xx/5xx — «нездоровый»
- DEFAULT_HEALTH (legacy): `/health`

Пример конфигурации (использование базового URL):

```yaml
sources:
  crossref:
    http:
      base_url: https://api.crossref.org/works
      # health_endpoint не указан — используется base_url
```

Пример со спец. эндпоинтом здоровья:

```yaml
sources:
  chembl:
    http:
      base_url: https://www.ebi.ac.uk/chembl/api/data
      health_endpoint: "status"  # проверка по /status
```

Запуск через CLI:

```bash
python -m library.cli health --config configs/config.yaml --timeout 15.0 --json
```

### Rate limiting и мониторинг

Для источников с лимитами запросов задайте параметры в конфигурации у соответствующего источника (`sources.<name>.rate_limit.max_calls|period`). Для регулярного мониторинга:

```bash
python -m library.tools.monitor_api
python -m library.tools.monitor_pubmed
python -m library.tools.monitor_semantic_scholar
```

Результаты и отчёты могут сохраняться в каталог `reports/`.
