# CLI

CLI основан на Typer. Ниже — частые команды и рецепты.

## Базовые команды

```bash
# помощь
bioactivity-data-acquisition --help

# запуск пайплайна
bioactivity-data-acquisition pipeline --config configs/config.yaml --set http.global.timeout_sec=10
```

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

# настроить HTTP таймауты/ретраи с CLI-оверрайдами
bioactivity-data-acquisition get-document-data --config configs/config_documents_full.yaml \
  --timeout-sec 10 --retries 3
```

Опции:

- `--documents-csv PATH` — входной CSV со списком идентификаторов документов
- `--output-dir PATH` — директория для артефактов
- `--date-tag YYYYMMDD` — тег даты в именах файлов
- `--timeout-sec N` — таймаут HTTP
- `--retries N` — число ретраев
- `--workers N` — число потоков
- `--limit N` — ограничение количества документов
- `--all` — включить все источники
- `--source NAME` — включить только указанные источники (можно повторять)
- `--dry-run/--no-dry-run` — выполнить без записи артефактов

## Версия

```bash
bioactivity-data-acquisition version
```

## Рецепты

```bash
# ограничить скорость для источника (пример ключа в конфиге)
bioactivity-data-acquisition pipeline --config configs/config.yaml --set api.chembl.rate_limit=5/s

# dry-run пайплайна (если поддерживается в конфиге)
bioactivity-data-acquisition pipeline --config configs/config.yaml --set runtime.dry_run=true
```

Подробная справка по флагам доступна через `--help` на подкомандах.
