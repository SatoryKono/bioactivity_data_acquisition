# Target Pipeline

Пайплайн для извлечения и обогащения данных о белковых мишенях (targets) из ChEMBL, UniProt и IUPHAR.

## Особенности

- **Обязательные источники**: Все источники (ChEMBL, UniProt, IUPHAR) являются обязательными
- **Единый интерфейс**: Соответствует архитектуре других пайплайнов проекта
- **Pydantic конфигурация**: Валидация конфигурации с помощью Pydantic
- **Детерминистический вывод**: Воспроизводимые результаты
- **QC и корреляции**: Анализ качества данных и корреляционный анализ
- **CLI интеграция**: Полная интеграция с Typer CLI

## Быстрый старт

### 1. Подготовка входных данных

Создайте CSV файл с идентификаторами targets:

```csv
target_chembl_id
CHEMBL240
CHEMBL251
CHEMBL262
CHEMBL273
CHEMBL284
```

### 2. Запуск через Makefile

```bash
# Создать пример данных и запустить пайплайн
make -f Makefile.target target-example

# Запуск в режиме dry-run
make -f Makefile.target target-dry-run

# Запуск в dev режиме (позволяет неполные источники)
make -f Makefile.target target-dev
```

### 3. Запуск через CLI

```bash
# Через Typer CLI
python -m library.cli get-target-data \
  --config configs/config_target_full.yaml \
  --targets-csv data/input/target_ids.csv \
  --date-tag 20251020

# Через Python скрипт
python -m library.scripts.get_target_data \
  --config configs/config_target_full.yaml \
  --targets-csv data/input/target_ids.csv \
  --output-dir data/output/target \
  --date-tag 20251020
```

## Конфигурация

### Основные настройки

```yaml
# configs/config_target_full.yaml
sources:
  chembl:
    enabled: true  # Обязательно
  uniprot:
    enabled: true  # Обязательно
  iuphar:
    enabled: true  # Обязательно

runtime:
  workers: 4
  limit: null  # Без ограничений
  dev_mode: false  # true для тестирования
  allow_incomplete_sources: false  # true для тестирования
```

### Обязательные источники

Все три источника являются обязательными:

- **ChEMBL**: Основные данные о targets
- **UniProt**: Обогащение белковыми данными
- **IUPHAR**: Фармакологическая классификация

В dev режиме можно отключить источники для тестирования:

```yaml
runtime:
  dev_mode: true
  allow_incomplete_sources: true
```

## Выходные файлы

Пайплайн создает следующие файлы:

```
data/output/target/
├── target_20251020.csv                    # Основные данные
├── target_20251020_qc.csv                 # QC метрики
├── target_20251020_meta.yaml              # Метаданные
└── target_correlation_report_20251020/    # Корреляционный анализ
    ├── correlation_matrix.csv
    └── correlation_insights.csv
```

### Основные поля

- **ChEMBL**: `target_chembl_id`, `pref_name`, `target_type`, `protein_classifications`
- **UniProt**: `uniprot_id_primary`, `molecular_function`, `cellular_component`
- **IUPHAR**: `iuphar_target_id`, `iuphar_type`, `gtop_function_text_short`

## API

### Основные функции

```python
from library.target import (
    TargetConfig,
    load_target_config,
    run_target_etl,
    write_target_outputs,
)

# Загрузка конфигурации
config = load_target_config("configs/config_target_full.yaml")

# Запуск ETL
result = run_target_etl(config, target_ids=["CHEMBL240", "CHEMBL251"])

# Запись результатов
outputs = write_target_outputs(result, output_dir, date_tag, config)
```

### Конфигурация

```python
from library.target import TargetConfig

# Создание конфигурации
config = TargetConfig(
    runtime={"workers": 8, "limit": 100},
    sources={
        "chembl": {"enabled": True},
        "uniprot": {"enabled": True},
        "iuphar": {"enabled": True},
    }
)

# Dev режим для тестирования
config = TargetConfig(runtime={"dev_mode": True})
```

## Тестирование

```bash
# Запуск тестов
make -f Makefile.target test-target

# Проверка конфигурации
make -f Makefile.target validate-target-config

# Проверка зависимостей
make -f Makefile.target check-target-deps
```

## Миграция со старого API

Старый API в `library.pipelines.target` помечен как устаревший:

```python
# Старый API (устаревший)
from library.pipelines.target.pipeline import run_pipeline

# Новый API
from library.target import run_target_etl
```

### Изменения в API

1. **Единая функция**: `run_target_etl()` вместо `run_pipeline()`
2. **Pydantic конфигурация**: `TargetConfig` вместо dataclasses
3. **Обязательные источники**: Все источники включены по умолчанию
4. **Стандартизированный результат**: `TargetETLResult` с QC и метаданными

## Troubleshooting

### Ошибка "All sources must be enabled"

Все источники обязательны. Для тестирования используйте dev режим:

```bash
python -m library.scripts.get_target_data \
  --config configs/config_target_full.yaml \
  --targets-csv data/input/target_ids.csv \
  --dev-mode
```

### Ошибка валидации входных данных

Убедитесь, что CSV файл содержит колонку `target_chembl_id`:

```csv
target_chembl_id
CHEMBL240
CHEMBL251
```

### Проблемы с API

- ChEMBL: Работает без API ключа с ограничениями
- UniProt: Работает без API ключа
- IUPHAR: Использует CSV словари или API fallback

## Дополнительные команды

```bash
# Показать справку
make -f Makefile.target help

# Показать примеры использования
make -f Makefile.target target-examples

# Показать справку по конфигурации
make -f Makefile.target target-config-help

# Показать статистику
make -f Makefile.target target-stats

# Очистить выходные файлы
make -f Makefile.target clean-target
```
