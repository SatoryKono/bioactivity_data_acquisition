# E2E: Обогащение документов

## Подготовка
```bash
python -m venv .venv
. .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Конфиг
Используем `configs/config_documents_full.yaml` и вход `data/input/documents.csv`.

## Запуск
```bash
bioactivity-data-acquisition get-document-data \
  --config configs/config_documents_full.yaml \
  --documents-csv data/input/documents.csv \
  --output-dir data/output/full \
  --date-tag 20251016 --all --limit 100
```

## Ожидаемые артефакты
- `data/output/full/*_documents.csv`
- `data/output/full/*_documents_qc_report.csv`
- `data/output/full/*_documents_correlation.csv`
