<!-- Generated from template: docs/_templates/staging_template.md -->

## S00 Spec

- Цель: обогащение метаданных публикаций для входного списка `documents.csv`.
- Вход: CSV `data/input/documents.csv` (см. `library.documents.config.DocumentInputSettings`). Обязательные колонки: `document_chembl_id`, `doi`, `title` (см. `library.documents.pipeline._REQUIRED_COLUMNS`).
- Выход: детерминированные CSV и мета-артефакты в `data/output/documents/`.

## S01 Extract

- Источники (включаемые через конфиг): chembl, crossref, openalex, pubmed, semantic_scholar (см. `configs/config.yaml`, `library.documents.config.ALLOWED_SOURCES`).
- Параметры и пагинация: см. `configs/config.yaml → sources.*.pagination`.
- Лимиты/ретраи: `http.global.retries`, `sources.*.http.retries` и `sources.*.rate_limit`.
- Заголовки: плейсхолдеры вида `{SEMANTIC_SCHOLAR_API_KEY}` (см. `library.documents.pipeline._create_api_client`).

## S02 Raw-Schema

- Pandera схема сырья: не выделена как отдельная SchemaModel; валидация входных колонок реализована в `library.documents.pipeline._normalise_columns`.

## S03 Normalize

- Правила (вход→выход): нормализация `document_chembl_id`, `doi`, `title`; маппинг устаревших полей (`classification` → `document_classification`, и т.д.) — см. `library.documents.pipeline._normalise_columns`.
- Обогащение по источникам — см. `_extract_data_from_source`.

## S04 Validate

- Проверки: обязательные колонки, отсутствие дублей по `document_chembl_id`; валидация полей после постобработки — `library.tools.data_validator.validate_all_fields`.

## S05 QC

- Метрики: `row_count`, `enabled_sources`, per-source counts; корреляционные отчёты — опционально (см. `postprocess.correlation`).
- Пути: `data/output/documents/documents_{date_tag}_qc.csv`, каталог `documents_correlation_report_{date_tag}`.

## S06 Persist

- Формат: CSV (+ YAML мета).
- Порядок/NA: детерминированная сериализация через `library.etl.load.write_deterministic_csv`; столбцы и сортировка настраиваются `determinism.*` в YAML.
- Метаданные: YAML с `file_checksums` (SHA256), счётчиками строк, версией ChEMBL.

## S07 CLI

- Команда: `bioactivity-data-acquisition get-document-data` (см. `library.cli:get_document_data`).
- Параметры (≤10): `--config`, `--documents-csv`, `--output-dir`, `--date-tag`, `--timeout-sec`, `--retries`, `--workers`, `--limit`, `--source/--all-sources`, `--dry-run`.
- Примеры:
  - `bioactivity-data-acquisition get-document-data -c configs/config.yaml --set runtime.workers=8`
  - `BIOACTIVITY__HTTP__GLOBAL__TIMEOUT_SEC=60 bioactivity-data-acquisition get-document-data -c configs/config.yaml`

## S08 Ops/CI

- CI собирает сайт MkDocs и публикует (см. `configs/mkdocs.yml`, GitHub Actions workflow).
- Воспроизводимость: фиксация версий зависимостей в `pyproject.toml`, детерминизм в конфиге (`determinism.*`), логирование в `logs/app.log`.

