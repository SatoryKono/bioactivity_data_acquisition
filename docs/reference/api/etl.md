# ETL пайплайн

Основные компоненты процесса извлечения, трансформации и загрузки данных.

## Основные функции

### Запуск пайплайна

`library.etl.run.run_pipeline(config, logger)` — выполняет полный цикл ETL.

## Модули ETL

### Извлечение данных

`library.etl.extract` — функции для извлечения данных из источников.

### Трансформация данных

`library.etl.transform` — нормализация, валидация и преобразования.

### Загрузка данных

`library.etl.load` — запись детерминированных CSV и артефактов.

### Контроль качества

`library.etl.qc` — расчёт QC-метрик и отчётов.

### Расширенный QC

`library.etl.enhanced_qc` — дополнительные проверки качества.

### Корреляционный анализ

`library.etl.enhanced_correlation` — корреляции и визуализация.

## Примеры использования

### Полный пайплайн

```python
from library.etl.run import run_pipeline
from library.config import Config
from library.logging_setup import get_logger

config = Config.from_yaml("configs/config.yaml")
logger = get_logger("pipeline")

# Запуск полного ETL
output_path = run_pipeline(config, logger)
```

### Отдельные этапы

```python
from library.etl.extract import fetch_bioactivity_data
from library.etl.transform import normalize_bioactivity_data
from library.etl.load import write_deterministic_csv

# Извлечение
raw_data = fetch_bioactivity_data(client, logger)

# Трансформация
normalized_data = normalize_bioactivity_data(
    raw_data, 
    transforms=config.transforms,
    determinism=config.determinism,
    logger=logger
)

# Загрузка
write_deterministic_csv(
    normalized_data,
    output_path,
    logger=logger,
    determinism=config.determinism,
    output=config.io.output
)
```
