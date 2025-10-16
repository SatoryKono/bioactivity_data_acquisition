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

- `data/output/full/documents_<YYYYMMDD>.csv`
- `data/output/full/documents_<YYYYMMDD>_qc.csv`
- (если включён корреляционный анализ) каталог `data/output/full/documents_correlation_report_<YYYYMMDD>/` с CSV/JSON файлами

Пример структуры корреляционных артефактов:

```text
data/output/full/
  documents_20251016.csv
  documents_20251016_qc.csv
  documents_correlation_report_20251016/
    correlation_insights.json
    numeric_pearson.csv
    numeric_spearman.csv
    numeric_covariance.csv
    categorical_cramers_v.csv
    mixed_eta_squared.csv
    mixed_point_biserial.csv
```
