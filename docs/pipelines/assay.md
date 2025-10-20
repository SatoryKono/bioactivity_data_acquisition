<!-- Generated from template: docs/_templates/staging_template.md -->

## S00 Spec

- Цель: извлечение и нормализация данных об ассаях ChEMBL, сохранение детерминированных артефактов.
- Вход: CSV с колонкой `assay_chembl_id` или параметры `assay_ids`/`target_chembl_id` + фильтры.
- Выход: `data/output/assay/assays_{date_tag}.csv` + QC + meta + корреляции.

## S01 Extract

- Источники: ChEMBL — батчевые запросы, fallback на одиночные (см. `library.assay.pipeline._extract_assay_data*`).
- Параметры/лимиты/ретраи — через `AssayConfig.sources.chembl.http` и `http.global`.
- Унифицированная фабрика клиентов: `library.clients.factory.create_api_client`.

## S02 Raw-Schema

- Входные требования: `assay_chembl_id` обязательный — `library.assay.pipeline._normalise_columns`.

## S03 Normalize

- Нормализация строк/списков, сопоставления `assay_type` → описание, ограничения диапазонов — `library.assay.pipeline._normalize_assay_fields`.

## S04 Validate

- Pandera: `library.schemas.assay_schema.AssayNormalizedSchema.validate` + бизнес-правила.
- Опциональная валидация данных при записи через `data_validator.validate_all_fields`.

## S05 QC

- Метрики: `row_count`, `enabled_sources`, `chembl_records`.
- Корреляционный анализ: `postprocess.correlation.enabled`.

## S06 Persist

- CSV + YAML meta; детерминированная запись `library.etl.load.write_deterministic_csv`.
- Deprecated: `io.output.format/csv/parquet` (используйте `write_deterministic_csv`).

## S07 CLI

- Новая команда: `get-assay-data --config config.yaml --input data.csv --output-dir results/`
- Deprecated: `assay-run` (будет удалена в следующей версии)
- Единые флаги: `--config`, `--input`, `--output-dir`, `--date-tag`, `--timeout-sec`, `--retries`, `--workers`, `--limit`, `--dry-run`

## S08 Ops/CI

- Аналогично другим пайплайнам: логи, артефакты, воспроизводимость.

## Установка и настройка

### Установка зависимостей

```bash
pip install -r requirements.txt
```

### API ключ ChEMBL (опционально)

Для повышения лимитов API можно установить переменную окружения:

```bash
# Windows (PowerShell)
$env:CHEMBL_API_TOKEN = "your_chembl_token_here"

# Linux/macOS (bash)
export CHEMBL_API_TOKEN="your_chembl_token_here"
```

## Быстрый старт

```bash
# Запуск с примером данных
python src/scripts/get_assay_data.py \
    --input data/input/assay_ids_example.csv \
    --config configs/config_assay_full.yaml
```

### Makefile (если используется)

```bash
make -f Makefile.assay assay-example
make -f Makefile.assay validate-assay-config
make -f Makefile.assay assay-status
```

## Использование CLI

### Извлечение по идентификаторам ассев

```bash
python src/scripts/get_assay_data.py \
    --input data/input/assay_ids_example.csv \
    --config configs/config_assay_full.yaml
```

### Извлечение по идентификатору таргета

```bash
python src/scripts/get_assay_data.py \
    --target CHEMBL231 \
    --config configs/config_assay_full.yaml
```

### Использование профилей фильтрации

```bash
python src/scripts/get_assay_data.py \
    --target CHEMBL231 \
    --filters human_single_protein \
    --config configs/config_assay_full.yaml
```

### Полезные параметры

```bash
# Ограничение количества записей
python src/scripts/get_assay_data.py --target CHEMBL231 --limit 100 --config configs/config_assay_full.yaml

# Таймауты и повторы
python src/scripts/get_assay_data.py --target CHEMBL231 --timeout 120 --retries 15 --config configs/config_assay_full.yaml

# Выходная директория
python src/scripts/get_assay_data.py --target CHEMBL231 --output-dir data/output/custom --config configs/config_assay_full.yaml

# Режим тестирования
python src/scripts/get_assay_data.py --target CHEMBL231 --dry-run --config configs/config_assay_full.yaml
```

## Профили фильтрации (фрагменты конфига)

```yaml
human_single_protein:
  target_organism: "Homo sapiens"
  target_type: "SINGLE PROTEIN"
  relationship_type: "D"
  confidence_score__range: "7,9"
  assay_type__in: "B,F"

binding_assays:
  assay_type: "B"
  relationship_type: "D"
  confidence_score__range: "5,9"

functional_assays:
  assay_type: "F"
  relationship_type: "D"
  confidence_score__range: "5,9"

high_quality:
  confidence_score__range: "7,9"
  relationship_type: "D"
  assay_type__in: "B,F"
```

## Выходные файлы

- `assay_YYYYMMDD.csv` — основные данные ассев
- `assay_YYYYMMDD_qc.csv` — отчёт о качестве данных
- `assay_YYYYMMDD_meta.yaml` — метаданные пайплайна

## Программное использование

```python
from library.assay import AssayConfig, load_assay_config, run_assay_etl, write_assay_outputs
from pathlib import Path

config = load_assay_config("configs/config_assay_full.yaml")
result = run_assay_etl(config=config, assay_ids=["CHEMBL123456", "CHEMBL789012"])
output_paths = write_assay_outputs(result=result, output_dir=Path("data/output/assay"), date_tag="20230101", config=config)
```

## Тестирование

```bash
pytest tests/test_assay_pipeline.py -v
pytest tests/test_assay_pipeline.py::TestAssayPipeline::test_normalise_columns_valid_input -v
pytest tests/test_assay_pipeline.py --cov=library.assay --cov-report=html
```

## Устранение неполадок

1. Ошибка «API Error»: проверьте сеть, увеличьте таймаут, проверьте лимиты API
2. Ошибка валидации: проверьте формат входных данных и обязательные поля
3. Медленная работа: используйте `--limit`, включите кэширование, профили фильтрации

