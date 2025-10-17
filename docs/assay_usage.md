# Использование пайплайна Assay

## Обзор

Пайплайн Assay предназначен для извлечения и нормализации данных ассев из ChEMBL API. Он следует архитектуре проекта и обеспечивает детерминированную обработку данных с полной валидацией.

## Установка и настройка

### 1. Установка зависимостей

Убедитесь, что все зависимости установлены:

```bash
pip install -r requirements.txt
```

### 2. Настройка API ключей (опционально)

Для повышения лимитов API можно установить переменные окружения:

```bash
# Windows (PowerShell)
$env:CHEMBL_API_TOKEN = "your_chembl_token_here"

# Linux/macOS (bash)
export CHEMBL_API_TOKEN="your_chembl_token_here"
```

**Примечание**: Пайплайн работает без API ключей, но с ограничениями по rate limiting.

### 3. Подготовка входных данных

Создайте CSV файл с идентификаторами ассев:

```csv
assay_chembl_id
CHEMBL123456
CHEMBL789012
CHEMBL345678
```

Или CSV файл с идентификаторами таргетов:

```csv
target_chembl_id
CHEMBL231
CHEMBL232
CHEMBL233
```

## Использование CLI

### Базовые команды

#### Извлечение по идентификаторам ассев

```bash
python src/scripts/get_assay_data.py \
    --input data/input/assay_ids_example.csv \
    --config configs/config_assay_full.yaml
```

#### Извлечение по идентификатору таргета

```bash
python src/scripts/get_assay_data.py \
    --target CHEMBL231 \
    --config configs/config_assay_full.yaml
```

#### Использование профилей фильтрации

```bash
python src/scripts/get_assay_data.py \
    --target CHEMBL231 \
    --filters human_single_protein \
    --config configs/config_assay_full.yaml
```

### Дополнительные параметры

#### Ограничение количества записей

```bash
python src/scripts/get_assay_data.py \
    --target CHEMBL231 \
    --limit 100 \
    --config configs/config_assay_full.yaml
```

#### Настройка таймаутов и повторов

```bash
python src/scripts/get_assay_data.py \
    --target CHEMBL231 \
    --timeout 120 \
    --retries 15 \
    --config configs/config_assay_full.yaml
```

#### Указание выходной директории

```bash
python src/scripts/get_assay_data.py \
    --target CHEMBL231 \
    --output-dir data/output/custom \
    --config configs/config_assay_full.yaml
```

#### Режим тестирования

```bash
python src/scripts/get_assay_data.py \
    --target CHEMBL231 \
    --dry-run \
    --config configs/config_assay_full.yaml
```

### Коды возврата

- **0**: Успешное выполнение
- **1**: Прерывание пользователем
- **2**: Ошибка валидации данных
- **3**: Ошибка API или сети

## Профили фильтрации

В конфигурации доступны следующие профили фильтрации:

### human_single_protein

```yaml
target_organism: "Homo sapiens"
target_type: "SINGLE PROTEIN"
relationship_type: "D"
confidence_score__range: "7,9"
assay_type__in: "B,F"
```

### binding_assays

```yaml
assay_type: "B"
relationship_type: "D"
confidence_score__range: "5,9"
```

### functional_assays

```yaml
assay_type: "F"
relationship_type: "D"
confidence_score__range: "5,9"
```

### high_quality

```yaml
confidence_score__range: "7,9"
relationship_type: "D"
assay_type__in: "B,F"
```

## Выходные файлы

Пайплайн создает следующие файлы:

### assay_YYYYMMDD.csv

Основной файл с данными ассев в CSV формате.

### assay_YYYYMMDD_qc.csv

Отчет о качестве данных с метриками.

### assay_YYYYMMDD_meta.yaml

Метаданные пайплайна:

- Версия пайплайна
- Версия ChEMBL
- Количество записей
- Контрольные суммы файлов
- Параметры извлечения

## Программное использование

### Базовый пример

```python
from library.assay import AssayConfig, load_assay_config, run_assay_etl, write_assay_outputs
from pathlib import Path

# Загрузка конфигурации
config = load_assay_config("configs/config_assay_full.yaml")

# Запуск ETL
result = run_assay_etl(
    config=config,
    assay_ids=["CHEMBL123456", "CHEMBL789012"]
)

# Сохранение результатов
output_paths = write_assay_outputs(
    result=result,
    output_dir=Path("data/output/assay"),
    date_tag="20230101",
    config=config
)

print(f"Созданы файлы: {output_paths}")
```

### Извлечение по таргету

```python
# Извлечение ассев для конкретного таргета
result = run_assay_etl(
    config=config,
    target_chembl_id="CHEMBL231",
    filters={
        "relationship_type": "D",
        "confidence_score__range": "7,9",
        "assay_type__in": "B,F"
    }
)
```

### Работа с результатами

```python
# Доступ к данным
assays_df = result.assays
qc_df = result.qc
meta = result.meta

# Анализ данных
print(f"Всего ассев: {len(assays_df)}")
print(f"Уникальных источников: {assays_df['src_id'].nunique()}")
print(f"Типы ассев: {assays_df['assay_type'].value_counts().to_dict()}")
```

## Конфигурация

### Основные настройки

```yaml
# HTTP настройки
http:
  global:
    timeout_sec: 60.0
    retries:
      total: 10
      backoff_multiplier: 3.0
    rate_limit:
      max_calls: 3
      period: 15.0

# Входные данные
io:
  input:
    assay_ids_csv: data/input/assay_ids.csv
    target_ids_csv: data/input/target_ids.csv
  output:
    dir: data/output/assay
    format: csv

# Валидация
validation:
  strict: true
  qc:
    max_missing_fraction: 0.02
    max_duplicate_fraction: 0.005
```

### Настройка детерминизма

```yaml
determinism:
  sort:
    by:
      - assay_chembl_id
    ascending:
      - true
    na_position: last
  column_order:
    - index
    - assay_chembl_id
    - src_id
    - src_name
    # ... остальные колонки
```

## Тестирование

### Запуск тестов

```bash
# Все тесты
pytest tests/test_assay_pipeline.py -v

# Конкретный тест
pytest tests/test_assay_pipeline.py::TestAssayPipeline::test_normalise_columns_valid_input -v

# С покрытием
pytest tests/test_assay_pipeline.py --cov=library.assay --cov-report=html
```

### Тестовые данные

Используйте примеры файлов:

- `data/input/assay_ids_example.csv` - пример идентификаторов ассев
- `data/input/target_ids_example.csv` - пример идентификаторов таргетов

## Мониторинг и отладка

### Логирование

```bash
# Увеличение детализации логов
python src/scripts/get_assay_data.py \
    --target CHEMBL231 \
    --log-level DEBUG \
    --config configs/config_assay_full.yaml
```

### Проверка статуса ChEMBL

```python
from library.assay.client import AssayChEMBLClient
from library.config import APIClientConfig

config = APIClientConfig(name="test", base_url="https://www.ebi.ac.uk/chembl/api/data")
client = AssayChEMBLClient(config)

status = client.get_chembl_status()
print(f"ChEMBL release: {status['chembl_release']}")
print(f"Status: {status['status']}")
```

## Устранение неполадок

### Частые проблемы

1. **Ошибка "API Error"**
   - Проверьте подключение к интернету
   - Увеличьте таймаут в конфигурации
   - Проверьте лимиты API

2. **Ошибка валидации схемы**
   - Проверьте формат входных данных
   - Убедитесь, что все обязательные поля присутствуют

3. **Медленная работа**
   - Уменьшите количество записей с помощью `--limit`
   - Настройте кэширование в конфигурации
   - Используйте профили фильтрации для уменьшения объема данных

### Получение помощи

```bash
# Справка по команде
python src/scripts/get_assay_data.py --help

# Примеры использования
python src/scripts/get_assay_data.py --help | grep -A 20 "Examples:"
```

## Производительность

### Рекомендации

1. **Используйте профили фильтрации** для уменьшения объема данных
2. **Настройте кэширование** для повторных запросов
3. **Ограничивайте количество записей** при тестировании
4. **Используйте Parquet формат** для больших объемов данных

### Мониторинг

```python
import time

start_time = time.time()
result = run_assay_etl(config=config, assay_ids=assay_ids)
end_time = time.time()

print(f"Время выполнения: {end_time - start_time:.2f} секунд")
print(f"Скорость: {len(result.assays) / (end_time - start_time):.2f} ассев/сек")
```
