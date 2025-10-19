## Activity: запуск и использование

- Базовый конфиг: `configs/config_activity_full.yaml`
- Переменные окружения с префиксом `BIOACTIVITY__` переопределяют YAML, например:
  - `BIOACTIVITY__HTTP__GLOBAL__TIMEOUT_SEC=60`
  - `BIOACTIVITY__SOURCES__CHEMBL__RATE_LIMIT__MAX_CALLS=2`

Пример кода:

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
