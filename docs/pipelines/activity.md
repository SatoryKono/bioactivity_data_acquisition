<!-- Generated from template: docs/_templates/staging_template.md -->

## S00 Spec
- Цель: нормализация и экспорт таблиц биоактивностей (если реализовано в коде).
- Вход/выход: по конфигу `configs/config_activity_full.yaml` (если используется).

## S01 Extract
- Источники: согласно конфигу; ограничение — описывать только реально используемые клиенты.

## S02 Raw-Schema
- Pandera-схемы: см. `src/library/schemas/activity_schema.py` (если применяется в пайплайне).

## S03 Normalize
- Правила нормализации: зафиксировать после интеграции фактических функций (пока каркас).

## S04 Validate
- Проверки типов/бизнес-правила: использовать `activity_schema` при наличии.

## S05 QC
- Метрики/пороги и пути репортов — по аналогии с другими пайплайнами.

## S06 Persist
- CSV + YAML meta; детерминизм как в остальной системе.

## S07 CLI
- Использование через общую команду `pipeline` и соответствующий конфиг.

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

