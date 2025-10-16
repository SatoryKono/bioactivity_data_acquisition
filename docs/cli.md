# CLI

CLI основан на Typer. Ниже — частые команды и рецепты.

## Базовые команды

```bash
# помощь
bioactivity-data-acquisition --help

# запуск пайплайна
bioactivity-data-acquisition pipeline --config configs/config.yaml \
  --set http.global.timeout_sec=10
```

### Опции команды `pipeline`

- `--config, -c PATH` — путь к YAML-конфигу
- `--set KEY=VALUE` (повторяемый) — переопределение значений по «точечным» путям. Пример:
  - `--set logging.level=DEBUG`
  - `--set sources.chembl.pagination.max_pages=1`

## Работа с документами (обогащение)

```bash
# минимальный запуск (пути берутся из конфига если указаны)
bioactivity-data-acquisition get-document-data --config configs/config_documents_full.yaml \
  --documents-csv data/input/documents.csv \
  --output-dir data/output/full \
  --date-tag 20251016

# включить все источники
bioactivity-data-acquisition get-document-data --config configs/config_documents_full.yaml --all

# включить только перечисленные источники
bioactivity-data-acquisition get-document-data --config configs/config_documents_full.yaml \
  --source chembl --source crossref

# ограничить количество документов и включить dry-run
bioactivity-data-acquisition get-document-data --config configs/config_documents_full.yaml \
  --limit 50 --dry-run

# настроить HTTP таймауты/ретраи через CLI-оверрайды конфигурации
bioactivity-data-acquisition get-document-data --config configs/config_documents_full.yaml \
  --timeout-sec 10 --retries 3
```

### Опции команды `get-document-data`

- `--config, -c PATH` — путь к YAML-конфигу
- `--documents-csv PATH` — входной CSV со списком идентификаторов документов
- `--output-dir PATH` — директория для артефактов
- `--date-tag YYYYMMDD` — тег даты в именах файлов
- `--timeout-sec N` — таймаут HTTP (попадает в `http.global.timeout_sec`)
- `--retries N` — число ретраев (попадает в `http.global.retries.total`)
- `--workers N` — число потоков (попадает в `runtime.workers`)
- `--limit N` — ограничение количества документов (`runtime.limit`)
- `--all` — включить все источники
- `--source NAME` — включить только указанные источники (можно повторять)
- `--dry-run/--no-dry-run` — выполнить без записи артефактов

Взаимоисключение: `--all` и `--source` нельзя использовать одновременно.

## Версия

```bash
bioactivity-data-acquisition version
```

## Рецепты

```bash
# Переопределение лимитов/таймаутов через --set
bioactivity-data-acquisition pipeline --config configs/config.yaml \
  --set http.global.timeout_sec=45 \
  --set sources.chembl.pagination.max_pages=1

# Rate limit на источник через YAML (пример ключей в конфиге)
# sources.<name>.http.rate_limit.max_calls / period

# Dry-run (если поддерживается профилем документов)
bioactivity-data-acquisition get-document-data --config configs/config_documents_full.yaml \
  --dry-run
```

Подробная справка по флагам доступна через `--help` на подкомандах.
