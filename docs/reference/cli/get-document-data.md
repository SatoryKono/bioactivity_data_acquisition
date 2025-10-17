# Команда get-document-data

Обогащение документов данными из множественных API источников.

## Синтаксис

```bash
bioactivity-data-acquisition get-document-data [OPTIONS]
```

## Опции

| Опция | Тип | Описание | По умолчанию |
|-------|-----|----------|--------------|
| `--config`, `-c` | PATH | Путь к YAML-конфигурации | Обязательно |
| `--documents-csv` | PATH | Путь к CSV с документами | Из конфига |
| `--output-dir` | PATH | Директория для выходных файлов | Из конфига |
| `--date-tag` | YYYYMMDD | Тег даты для файлов | Обязательно |
| `--all` | - | Включить все источники | - |
| `--source` | NAME | Включить конкретный источник | - |
| `--limit` | N | Ограничить количество документов | - |
| `--dry-run` | - | Тестовый режим без записи | - |
| `--timeout-sec` | SEC | Таймаут HTTP запросов | Из конфига |
| `--retries` | N | Количество повторных попыток | Из конфига |

## Примеры

### Минимальный запуск

```bash
bioactivity-data-acquisition get-document-data \
  --config configs/config_documents_full.yaml \
  --documents-csv data/input/documents.csv \
  --output-dir data/output/full \
  --date-tag 20250101
```

### Включить все источники

```bash
bioactivity-data-acquisition get-document-data \
  --config configs/config_documents_full.yaml \
  --all \
  --date-tag 20250101
```

### Ограничить количество документов

```bash
bioactivity-data-acquisition get-document-data \
  --config configs/config_documents_full.yaml \
  --all \
  --limit 100 \
  --date-tag 20250101
```

### Выбрать конкретные источники

```bash
bioactivity-data-acquisition get-document-data \
  --config configs/config_documents_full.yaml \
  --source chembl \
  --source crossref \
  --date-tag 20250101
```

### Dry-run режим

```bash
bioactivity-data-acquisition get-document-data \
  --config configs/config_documents_full.yaml \
  --all \
  --dry-run \
  --date-tag 20250101
```

## Выходные файлы

Команда создаёт следующие файлы:

- `{output_dir}/documents_{date_tag}.csv` — обогащённые документы
- `{output_dir}/documents_{date_tag}_qc.csv` — QC отчёт
- `{output_dir}/documents_correlation_report_{date_tag}/` — корреляционные отчёты

## Поддерживаемые источники

- `chembl` — ChEMBL API
- `crossref` — Crossref API
- `openalex` — OpenAlex API
- `pubmed` — PubMed API
- `semantic_scholar` — Semantic Scholar API

## Коды возврата

- `0` — успешное выполнение
- `1` — ошибка конфигурации
- `2` — ошибка чтения входных данных
- `3` — ошибка API запросов
- `4` — ошибка записи файлов
