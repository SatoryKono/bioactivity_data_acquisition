<!-- Generated from template: docs/_templates/staging_template.md -->

## S00 Spec
- Цель: извлечение и нормализация данных о биоактивностях ChEMBL, сохранение детерминированных артефактов.
- Вход: CSV с колонкой `activity_chembl_id`.
- Выход: `data/output/activity/activities_{date_tag}.csv` + QC + meta + корреляции.

## S01 Extract
- Источники: ChEMBL — batch-запросы для эффективности (см. `library.activity.pipeline._extract_activity_data`).
- Параметры/лимиты/ретраи — через `ActivityConfig.sources.chembl.http` и `http.global`.
- Унифицированная фабрика клиентов: `library.clients.factory.create_api_client`.

## S02 Raw-Schema
- Входные требования: `activity_chembl_id` обязательный — `library.activity.pipeline._normalise_columns`.

## S03 Normalize
- Нормализация строк/списков, приведение типов, ограничения диапазонов — `library.activity.pipeline._normalize_activity_fields`.
- Полные SHA256 хеши для `hash_row` и `hash_business_key`.

## S04 Validate
- Pandera: `library.schemas.activity_schema.ActivityNormalizedSchema.validate` + бизнес-правила.
- Опциональная валидация данных при записи через `data_validator.validate_all_fields`.

## S05 QC
- Метрики: `row_count`, `enabled_sources`, `chembl_records`.
- Корреляционный анализ: `postprocess.correlation.enabled` (по умолчанию отключен).

## S06 Persist
- CSV + YAML meta; детерминированная запись `library.etl.load.write_deterministic_csv`.
- Имена артефактов: `activities_*` (множественное число) с legacy-алиасами.

## S07 CLI
- Новая команда: `get-activity-data --config config.yaml --input data.csv --output-dir results/`
- Deprecated: `activity-run` (будет удалена в следующей версии)
- Единые флаги: `--config`, `--input`, `--output-dir`, `--date-tag`, `--timeout-sec`, `--retries`, `--workers`, `--limit`, `--dry-run`

## S08 Ops/CI
- Общие правила репозитория (логи, репродуцируемость, артефакты CI).

## Установка и настройка

### Установка зависимостей

```bash
pip install -r requirements.txt
```

### Переопределения через окружение

Переменные окружения с префиксом `BIOACTIVITY__` переопределяют YAML-конфиг, например:

- `BIOACTIVITY__HTTP__GLOBAL__TIMEOUT_SEC=60`
- `BIOACTIVITY__SOURCES__CHEMBL__RATE_LIMIT__MAX_CALLS=2`

## Использование

### Программный пример

```python
from pathlib import Path
import pandas as pd
from library.activity import load_activity_config, run_activity_etl, write_activity_outputs

cfg = load_activity_config("configs/config_activity_full.yaml")
input_df = pd.read_csv("data/input/activity.csv")
result = run_activity_etl(cfg, input_df)
paths = write_activity_outputs(result, Path(cfg.io.output.dir), date_tag="20240101", config=cfg)
print(paths)
```

