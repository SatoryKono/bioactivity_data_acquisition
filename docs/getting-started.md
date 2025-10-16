# Getting started

## Требования

- Python 3.11 (минимум 3.10)
- Токены/ключи для источников (опционально): ChEMBL, Crossref, OpenAlex, PubMed, Semantic Scholar

## Установка

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install --upgrade pip
pip install .[dev]
```

## Быстрый старт (CLI)

```bash
# Справка
bioactivity-data-acquisition --help

# Базовый запуск пайплайна
bioactivity-data-acquisition pipeline --config configs/config.yaml \
  --set http.global.timeout_sec=45

# Обогащение документов (включить все источники)
bioactivity-data-acquisition get-document-data \
  --config configs/config_documents_full.yaml \
  --documents-csv data/input/documents.csv \
  --output-dir data/output/full \
  --date-tag 20250101 --all --limit 100
```

## Переменные окружения (секреты)

```bash
# примеры для Linux/macOS
export CHEMBL_API_TOKEN=...
export PUBMED_API_KEY=...
export SEMANTIC_SCHOLAR_API_KEY=...

# глобальные overrides
export BIOACTIVITY__LOGGING__LEVEL=DEBUG
export BIOACTIVITY__HTTP__GLOBAL__TIMEOUT_SEC=60
```

Дополнительно:

- Конфигурация: configuration.md
- CLI: cli.md
- Артефакты и QC: outputs.md
