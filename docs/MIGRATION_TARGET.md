# Migration Guide: Target Pipeline

Руководство по миграции с старого API `library.pipelines.target` на новый API `library.target`.

## Обзор изменений

Target пайплайн был полностью рефакторен для соответствия архитектуре других пайплайнов проекта. Основные изменения:

- **Новая структура модулей**: `library.target` вместо `library.pipelines.target`
- **Pydantic конфигурация**: Валидация конфигурации
- **Единый ETL интерфейс**: `run_target_etl()` вместо `run_pipeline()`
- **Обязательные источники**: Все источники (ChEMBL, UniProt, IUPHAR) обязательны
- **Стандартизированные результаты**: `TargetETLResult` с QC и метаданными
- **CLI интеграция**: Полная интеграция с Typer CLI

## Временная шкала миграции

- **v1.x**: Старый API помечен как deprecated, но работает
- **v2.0.0**: Старый API будет удален

## Пошаговая миграция

### 1. Обновление импортов

**Старый код:**
```python
from library.pipelines.target.pipeline import run_pipeline
from library.pipelines.target.config import ApiCfg, UniprotMappingCfg
```

**Новый код:**
```python
from library.target import (
    run_target_etl,
    TargetConfig,
    load_target_config,
    TargetETLResult,
)
```

### 2. Обновление конфигурации

**Старый код:**
```python
from library.pipelines.target.config import ApiCfg, UniprotMappingCfg

api_cfg = ApiCfg(
    chembl_base="https://www.ebi.ac.uk/chembl/api/data",
    timeout_read=60.0
)

mapping_cfg = UniprotMappingCfg(
    enabled=True,
    mapping_file="path/to/mapping.csv"
)
```

**Новый код:**
```python
from library.target import TargetConfig, load_target_config

# Через YAML файл (рекомендуется)
config = load_target_config("configs/config_target_full.yaml")

# Или программно
config = TargetConfig(
    http={"global": {"timeout_sec": 60.0}},
    sources={
        "chembl": {"enabled": True},
        "uniprot": {"enabled": True, "params": {"mapping_file": "path/to/mapping.csv"}},
        "iuphar": {"enabled": True},
    }
)
```

### 3. Обновление ETL процесса

**Старый код:**
```python
from library.pipelines.target.pipeline import run_pipeline
from library.pipelines.target.chembl_target import get_targets
from library.pipelines.target.uniprot_target import enrich_targets_with_uniprot
from library.pipelines.target.iuphar_target import enrich_targets_with_iuphar

# Сложная логика с множественными fetcher функциями
result = run_pipeline(
    chunk_iterator=target_ids,
    chembl_cfg=api_cfg,
    chembl_fetcher=get_targets,
    uniprot_cfg=uniprot_cfg,
    uniprot_fetcher=enrich_targets_with_uniprot,
    iuphar_cfg=iuphar_cfg,
    iuphar_fetcher=enrich_targets_with_iuphar,
)
```

**Новый код:**
```python
from library.target import run_target_etl

# Простой единый интерфейс
result = run_target_etl(
    config=config,
    target_ids=["CHEMBL240", "CHEMBL251", "CHEMBL262"]
)

# Или с DataFrame
input_df = pd.DataFrame({"target_chembl_id": ["CHEMBL240", "CHEMBL251"]})
result = run_target_etl(config=config, input_frame=input_df)
```

### 4. Обновление обработки результатов

**Старый код:**
```python
# Результат - Mapping с опциональными источниками
result: PipelineResult = run_pipeline(...)

chembl_df = result.get("chembl", pd.DataFrame())
uniprot_df = result.get("uniprot", pd.DataFrame())
iuphar_df = result.get("iuphar", pd.DataFrame())

# Ручная обработка результатов
if not chembl_df.empty:
    # Обработка ChEMBL данных
    pass
```

**Новый код:**
```python
# Стандартизированный результат
result: TargetETLResult = run_target_etl(config, target_ids)

# Все данные уже объединены
targets_df = result.targets  # pd.DataFrame с объединенными данными
qc_df = result.qc           # QC метрики
meta = result.meta          # Метаданные

# Корреляционный анализ (опционально)
if result.correlation_analysis:
    correlation = result.correlation_analysis
```

### 5. Обновление записи результатов

**Старый код:**
```python
# Ручная запись результатов
chembl_df.to_csv("chembl_targets.csv", index=False)
uniprot_df.to_csv("uniprot_targets.csv", index=False)
iuphar_df.to_csv("iuphar_targets.csv", index=False)
```

**Новый код:**
```python
from library.target import write_target_outputs

# Детерминистическая запись с QC и метаданными
outputs = write_target_outputs(
    result=result,
    output_dir=Path("data/output/target"),
    date_tag="20251020",
    config=config
)

# outputs содержит пути к созданным файлам:
# - outputs["csv"]: основные данные
# - outputs["qc"]: QC метрики
# - outputs["meta"]: метаданные
# - outputs["correlation_*"]: корреляционные отчеты
```

### 6. Обновление CLI

**Старый код:**
```python
# Нет CLI интерфейса
```

**Новый код:**
```bash
# Через Typer CLI
python -m library.cli get-target-data \
  --config configs/config_target_full.yaml \
  --targets-csv data/input/target_ids.csv

# Через Python скрипт
python -m library.scripts.get_target_data \
  --config configs/config_target_full.yaml \
  --targets-csv data/input/target_ids.csv

# Через Makefile
make -f Makefile.target target-example
```

## Ключевые различия

### Обязательные источники

**Старый API**: Источники были опциональными
```python
# Можно было отключить источники
result = run_pipeline(
    chunk_iterator=target_ids,
    chembl_cfg=api_cfg,
    chembl_fetcher=get_targets,
    # uniprot_fetcher не указан - источник отключен
)
```

**Новый API**: Все источники обязательны
```python
# Все источники включены по умолчанию
config = TargetConfig()  # chembl, uniprot, iuphar все включены

# Для тестирования можно использовать dev режим
config = TargetConfig(runtime={"dev_mode": True})
```

### Валидация данных

**Старый API**: Нет валидации
```python
# Данные не валидировались
result = run_pipeline(...)
```

**Новый API**: Pandera схемы
```python
# Автоматическая валидация входных и выходных данных
result = run_target_etl(config, target_ids)
# Валидация происходит внутри run_target_etl()
```

### Обработка ошибок

**Старый API**: Базовые исключения
```python
try:
    result = run_pipeline(...)
except Exception as e:
    # Общая обработка ошибок
    pass
```

**Новый API**: Специализированные исключения
```python
from library.target import (
    TargetValidationError,
    TargetHTTPError,
    TargetQCError,
    TargetIOError,
)

try:
    result = run_target_etl(config, target_ids)
except TargetValidationError as e:
    # Ошибка валидации данных
    pass
except TargetHTTPError as e:
    # Ошибка HTTP запросов
    pass
except TargetQCError as e:
    # Ошибка контроля качества
    pass
except TargetIOError as e:
    # Ошибка ввода/вывода
    pass
```

## Примеры миграции

### Простой пример

**Старый код:**
```python
from library.pipelines.target.pipeline import run_pipeline
from library.pipelines.target.config import ApiCfg

api_cfg = ApiCfg(
    chembl_base="https://www.ebi.ac.uk/chembl/api/data",
    timeout_read=60.0
)

target_ids = ["CHEMBL240", "CHEMBL251", "CHEMBL262"]
result = run_pipeline(
    chunk_iterator=target_ids,
    chembl_cfg=api_cfg,
    chembl_fetcher=get_targets,
)

# Обработка результатов
chembl_df = result.get("chembl", pd.DataFrame())
chembl_df.to_csv("targets.csv", index=False)
```

**Новый код:**
```python
from library.target import load_target_config, run_target_etl, write_target_outputs
from pathlib import Path

# Загрузка конфигурации
config = load_target_config("configs/config_target_full.yaml")

# Запуск ETL
target_ids = ["CHEMBL240", "CHEMBL251", "CHEMBL262"]
result = run_target_etl(config, target_ids=target_ids)

# Запись результатов
outputs = write_target_outputs(
    result=result,
    output_dir=Path("data/output/target"),
    date_tag="20251020",
    config=config
)
```

### Продвинутый пример с кастомной конфигурацией

**Старый код:**
```python
from library.pipelines.target.pipeline import run_pipeline
from library.pipelines.target.config import ApiCfg, UniprotMappingCfg

api_cfg = ApiCfg(
    chembl_base="https://www.ebi.ac.uk/chembl/api/data",
    timeout_read=120.0
)

mapping_cfg = UniprotMappingCfg(
    enabled=True,
    mapping_file="custom_mapping.csv",
    mapping_columns={"chembl_id": "target_chembl_id", "uniprot_id": "uniprot_id"}
)

result = run_pipeline(
    chunk_iterator=target_ids,
    chembl_cfg=api_cfg,
    chembl_fetcher=get_targets,
    uniprot_cfg=uniprot_cfg,
    uniprot_fetcher=enrich_targets_with_uniprot,
    iuphar_cfg=iuphar_cfg,
    iuphar_fetcher=enrich_targets_with_iuphar,
)
```

**Новый код:**
```python
from library.target import TargetConfig, run_target_etl

# Создание кастомной конфигурации
config = TargetConfig(
    http={"global": {"timeout_sec": 120.0}},
    sources={
        "chembl": {"enabled": True},
        "uniprot": {
            "enabled": True,
            "params": {
                "mapping_file": "custom_mapping.csv",
                "mapping_columns": {
                    "chembl_id": "target_chembl_id",
                    "uniprot_id": "uniprot_id"
                }
            }
        },
        "iuphar": {"enabled": True},
    }
)

result = run_target_etl(config, target_ids=target_ids)
```

## Тестирование миграции

### 1. Проверка совместимости

```python
# Тест, что новый API работает с теми же данными
def test_migration_compatibility():
    target_ids = ["CHEMBL240", "CHEMBL251", "CHEMBL262"]
    
    # Старый API (если еще доступен)
    # old_result = run_pipeline(...)
    
    # Новый API
    config = load_target_config("configs/config_target_full.yaml")
    new_result = run_target_etl(config, target_ids=target_ids)
    
    # Проверка, что результаты содержат ожидаемые данные
    assert len(new_result.targets) > 0
    assert "target_chembl_id" in new_result.targets.columns
    assert "uniprot_id_primary" in new_result.targets.columns
    assert "iuphar_target_id" in new_result.targets.columns
```

### 2. Проверка конфигурации

```python
def test_config_migration():
    # Проверка, что конфигурация загружается корректно
    config = load_target_config("configs/config_target_full.yaml")
    
    assert config.sources["chembl"].enabled is True
    assert config.sources["uniprot"].enabled is True
    assert config.sources["iuphar"].enabled is True
    assert config.runtime.workers > 0
```

## Поддержка и помощь

### Deprecation Warnings

При использовании старого API вы увидите предупреждения:

```python
import warnings
warnings.filterwarnings("default", category=DeprecationWarning)

from library.pipelines.target import run_pipeline
# DeprecationWarning: The 'library.pipelines.target' module is deprecated...
```

### Отладка проблем

1. **Проверьте конфигурацию**:
   ```bash
   make -f Makefile.target validate-target-config
   ```

2. **Запустите в dev режиме**:
   ```bash
   python -m library.scripts.get_target_data --dev-mode
   ```

3. **Проверьте зависимости**:
   ```bash
   make -f Makefile.target check-target-deps
   ```

### Получение помощи

- Документация: `README_target.md`
- Примеры: `make -f Makefile.target target-examples`
- Тесты: `make -f Makefile.target test-target`

## Заключение

Миграция на новый API обеспечивает:

- ✅ Единообразие с другими пайплайнами
- ✅ Лучшую валидацию и обработку ошибок
- ✅ Детерминистические результаты
- ✅ QC и корреляционный анализ
- ✅ Полную CLI интеграцию
- ✅ Comprehensive тестирование

Рекомендуется завершить миграцию до версии 2.0.0, когда старый API будет удален.
