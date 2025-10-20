<!-- Generated from template: docs/_templates/staging_template.md -->

## S00 Spec
- Цель: извлечение и обогащение молекулярных данных (ChEMBL + PubChem), нормализация и детерминированный экспорт.
- Вход: CSV с идентификаторами молекул (`molecule_chembl_id` и/или `molregno`).
- Выход: `data/output/testitem/testitems_{date_tag}.csv` + QC + meta.

## S01 Extract
- Источники: ChEMBL (обязательно), PubChem (опционально) — см. `library.testitem.pipeline` и конфиг `testitem`.
- Лимиты/ретраи: через `http.global` и overrides источников.

## S02 Raw-Schema
- Проверки входа: наличие хотя бы одного идентификатора — `library.testitem.pipeline._normalise_columns`.

## S03 Normalize
- Правила: нормализация строковых/числовых/булевых/списковых полей; вычисление `hash_*` — `library.testitem.pipeline._normalize_testitem_data`.

## S04 Validate
- Бизнес-правила: консистентность идентификаторов, диапазоны, длины — `library.testitem.pipeline._validate_*`.

## S05 QC
- Метрики: `row_count`, счётчики источников, PubChem coverage, ошибки.

## S06 Persist
- CSV + YAML meta; детерминизм через `library.etl.load.write_deterministic_csv`.

## S07 CLI
- Новая команда: `get-testitem-data --config config.yaml --input data.csv --output-dir results/`
- Legacy команда: `testitem-run` (deprecated, будет удалена в следующей версии)
- Единые флаги: `--config`, `--input`, `--output-dir`, `--date-tag`, `--timeout-sec`, `--retries`, `--workers`, `--limit`, `--dry-run`
- Специфичные флаги: `--cache-dir`, `--pubchem-cache-dir`, `--disable-pubchem`

## S08 Ops/CI
- Публикация артефактов в CI, детерминизм и логирование аналогично documents.

---

## Развернутая документация

# Testitem ETL Pipeline

Воспроизводимый, детерминированный конвейер Sxx_testitem для формирования измерения testitem_dim по молекулам из ChEMBL с обогащением из PubChem.

## Описание

Конвейер реализует полный цикл ETL (Extract → Transform → Load) для извлечения и нормализации молекулярных данных:

- **Extract**: Извлечение данных из ChEMBL и PubChem APIs
- **Normalize**: Нормализация и стандартизация данных
- **Validate**: Валидация с использованием Pandera схем
- **Persist**: Детерминистическое сохранение в CSV формате

## Архитектура

### Этапы конвейера (S01-S10)

1. **S01**: Получение статуса ChEMBL и версии релиза
2. **S02**: Извлечение основных данных молекулы
3. **S03**: Извлечение данных о родительских/дочерних связях
4. **S04**: Извлечение свойств и классификаций
5. **S05**: Обогащение данными из PubChem
6. **S06**: Извлечение синонимов и кросс-ссылок
7. **S07**: Стандартизация молекулярных структур
8. **S08**: Нормализация данных
9. **S09**: Валидация с помощью Pandera
10. **S10**: Детерминистическое сохранение и метаданные

### Структура проекта

```
src/library/testitem/
├── __init__.py          # Публичный интерфейс модуля
├── config.py            # Pydantic конфигурация (documents-style)
└── pipeline.py          # Консолидированный ETL конвейер

src/library/schemas/
└── testitem_schema.py   # Pandera схемы валидации

configs/
└── config_testitem_full.yaml        # Конфигурационный файл

src/scripts/
└── get_testitem_data.py # Скрипт запуска
```

**Примечание**: Модули `extract.py`, `normalize.py`, `validate.py`, `persist.py` были консолидированы в `pipeline.py` для упрощения архитектуры и соответствия стандартам `documents` модуля.

## Использование

### CLI интерфейс

```bash
# Основная команда (--output необязательный, загружается из конфигурации)
python -m library.cli testitem-run \
  --config configs/config_testitem_full.yaml \
  --input data/input/testitem_keys.csv

# С дополнительными параметрами
python -m library.cli testitem-run \
  --config configs/config_testitem_full.yaml \
  --input data/input/testitem_keys.csv \
  --output data/output/testitem \
  --cache-dir .cache/chembl \
  --pubchem-cache-dir .cache/pubchem \
  --timeout 45 \
  --retries 7 \
  --limit 500 \
  --disable-pubchem false \
  --verbose

# Валидация конфигурации
python -m library.cli testitem-validate-config \
  --config configs/config_testitem_full.yaml

# Информация о конвейере
python -m library.cli testitem-info
```

### Прямой запуск скрипта

```bash
# Автоматически использует команду testitem-run (--output загружается из конфигурации)
python src/scripts/get_testitem_data.py \
  --config configs/config_testitem_full.yaml \
  --input data/input/testitem_keys.csv

# Или явно указать команду
python src/scripts/get_testitem_data.py testitem-run \
  --config configs/config_testitem_full.yaml \
  --input data/input/testitem_keys.csv \
  --output data/output/testitem
```

### Программный интерфейс

```python
from library.testitem.config import load_testitem_config
from library.testitem.pipeline import run_testitem_etl, write_testitem_outputs
from pathlib import Path

# Загрузка конфигурации (новый API)
config = load_testitem_config("configs/config_testitem_full.yaml")

# Запуск ETL конвейера с файлом входных данных
result = run_testitem_etl(config, input_path=Path("data/input/testitem.csv"))

# Сохранение результатов
output_paths = write_testitem_outputs(result, Path("data/output/testitem"), config)
```

**Миграция с старого API**:
```python
# Старый способ (deprecated)
config = TestitemConfig.from_file("config.yaml")
result = run_testitem_etl(config, input_data=dataframe)

# Новый способ (рекомендуется)
config = load_testitem_config("config.yaml")
result = run_testitem_etl(config, input_path=Path("input.csv"))
```

## Входные данные

### Формат входного CSV

Обязательные поля (хотя бы одно):
- `molecule_chembl_id` - ChEMBL идентификатор молекулы
- `molregno` - Регистрационный номер молекулы в ChEMBL

Опциональные поля:
- `parent_chembl_id` - ChEMBL идентификатор родительской молекулы
- `parent_molregno` - Регистрационный номер родительской молекулы
- `pubchem_cid` - PubChem идентификатор соединения

### Пример входного файла

```csv
molecule_chembl_id,molregno,parent_chembl_id,parent_molregno,pubchem_cid
CHEMBL25,1,CHEMBL25,1,2244
CHEMBL153,2,CHEMBL153,2,2244
CHEMBL154,3,CHEMBL154,3,2244
```

## Выходные данные

### Структура выходных файлов

```
data/output/testitem/
├── testitem__20240101.csv          # Основной датасет
├── meta.yaml                       # Метаданные
└── qc/
    └── testitem_20240101_qc.csv    # QC артефакты

logs/
└── testitem_20240101_1200.jsonl    # Структурированные логи

.cache/
├── chembl/                         # HTTP кэш ChEMBL
└── pubchem/                        # HTTP кэш PubChem
```

### Формат имени файла

Имя CSV файла формируется детерминистически по формуле:
```
testitem__{run_date}.csv
```

Где `run_date` = YYYYMMDD (UTC) из поля `extracted_at` на стадии S10.

### Схема данных testitem_dim

Выходной CSV содержит следующие группы полей:

#### Основные идентификаторы
- `molecule_chembl_id` - ChEMBL ID молекулы
- `molregno` - Регистрационный номер
- `pref_name` - Предпочтительное название
- `pref_name_key` - Ключ для сортировки

#### Родительские связи
- `parent_chembl_id` - ID родительской молекулы
- `parent_molregno` - Регистрационный номер родительской молекулы

#### Разработка лекарств
- `max_phase` - Максимальная фаза разработки
- `therapeutic_flag` - Флаг терапевтического средства
- `dosed_ingredient` - Флаг дозируемого ингредиента
- `first_approval` - Год первого одобрения

#### Молекулярные свойства
- `structure_type` - Тип структуры
- `molecule_type` - Тип молекулы
- `mw_freebase` - Молекулярная масса
- `alogp` - ALogP
- `hba` - Акцепторы водородных связей
- `hbd` - Доноры водородных связей
- `psa` - Полярная площадь поверхности
- `rtb` - Поворотные связи

#### PubChem данные
- `pubchem_cid` - PubChem CID
- `pubchem_molecular_formula` - Молекулярная формула
- `pubchem_molecular_weight` - Молекулярная масса
- `pubchem_canonical_smiles` - Канонические SMILES
- `pubchem_inchi` - InChI
- `pubchem_inchi_key` - InChI ключ

#### Стандартизированные структуры
- `standardized_inchi` - Стандартизированный InChI
- `standardized_inchi_key` - Стандартизированный InChI ключ
- `standardized_smiles` - Стандартизированные SMILES

#### Техслужебные поля
- `source_system` - Система-источник
- `chembl_release` - Версия ChEMBL
- `extracted_at` - Время извлечения
- `hash_row` - Хеш строки
- `hash_business_key` - Хеш бизнес-ключа

## Конфигурация

### Основные параметры

```yaml
# Версия пайплайна
pipeline_version: 1.1.0
allow_parent_missing: false
enable_pubchem: true

# Настройки ввода-вывода
io:
  input:
    testitem_keys_csv: data/input/testitem.csv
  output:
    dir: data/output/testitem

# Настройки выполнения
runtime:
  workers: 8
  batch_size: 200
  retries: 5
  timeout_sec: 30
  log_level: INFO
  cache_dir: .cache/chembl
  pubchem_cache_dir: .cache/pubchem

# Настройки детерминизма
determinism:
  sort:
    by: [molecule_chembl_id, molregno, pref_name_key]
    ascending: [true, true, true]
    na_position: last

# Настройки постобработки
postprocess:
  qc:
    enabled: true
    enhanced: true
  correlation:
    enabled: true
    enhanced: true

# Настройки логирования
logging:
  level: INFO
```

### Источники данных

```yaml
sources:
  chembl:
    name: chembl
    enabled: true
    endpoint: molecule
    rate_limit:
      max_calls: 10
      period: 15.0
    http:
      base_url: https://www.ebi.ac.uk/chembl/api/data
      timeout_sec: 60.0
      retries:
        total: 5
        backoff_multiplier: 2.0

  pubchem:
    name: pubchem
    enabled: true
    endpoint: compound
    rate_limit:
      max_calls: 5
      period: 10.0
    http:
      base_url: https://pubchem.ncbi.nlm.nih.gov/rest/pug
      timeout_sec: 45.0
      retries:
        total: 8
        backoff_multiplier: 2.5
```

**Важные изменения**:
- `rate_limit` теперь настраивается на уровне источника, а не глобально
- Убраны подсекции `csv`/`parquet` из `io.output`
- Добавлены секции `postprocess` и `logging`

## Детерминизм

Конвейер обеспечивает полную воспроизводимость результатов:

- **Детерминистическая сортировка** по `molecule_chembl_id`, `molregno`, `pref_name_key`
- **Фиксированный порядок колонок** согласно конфигурации
- **Детерминистические хеши** для строк и бизнес-ключей
- **Воспроизводимые имена файлов** на основе `extracted_at` (UTC)

Повторный запуск на тех же входах, релизах источников и той же дате `extracted_at` обязан давать байтово идентичный CSV и одинаковые QC-метрики.

## API ключи

Система работает без API ключей с ограничениями по rate limiting. Для получения API ключей:

- **ChEMBL**: https://chembl.gitbook.io/chembl-interface-documentation/web-services/chembl-data-web-services
- **PubChem**: Работает без ключа

Установка переменных окружения:
```bash
# Windows (PowerShell)
$env:CHEMBL_API_TOKEN = "your_chembl_token_here"

# Linux/macOS (bash)
export CHEMBL_API_TOKEN="your_chembl_token_here"
```

## Тестирование

```bash
# Запуск тестов
pytest tests/test_testitem_pipeline.py -v

# Запуск с покрытием
pytest tests/test_testitem_pipeline.py --cov=library.testitem --cov-report=html
```

## Логирование

Конвейер использует структурированное JSON-логирование:

```json
{
  "timestamp": "2024-01-01T12:00:00Z",
  "level": "INFO",
  "stage": "S02",
  "source": "ChEMBL",
  "endpoint": "molecule",
  "params": {"molecule_chembl_id": "CHEMBL25"},
  "elapsed_ms": 150,
  "status_code": 200,
  "cache_hit": false
}
```

## Обработка ошибок

Конвейер реализует graceful degradation:

- При недоступности ChEMBL API - возвращает пустые записи с информацией об ошибке
- При недоступности PubChem API - продолжает работу без обогащения
- При ошибках валидации - логирует предупреждения и продолжает обработку
- При критических ошибках - останавливает выполнение с соответствующим кодом выхода

## Производительность

- **Батчевая обработка** для оптимизации API запросов
- **HTTP кэширование** для повторных запросов
- **Rate limiting** для соблюдения лимитов API
- **Retry механизм** с экспоненциальной задержкой
- **Параллельная обработка** где возможно

## Мониторинг

Конвейер предоставляет детальную информацию о процессе:

- QC метрики по каждому этапу
- Статистика по источникам данных
- Время выполнения операций
- Успешность обогащения PubChem
- Количество ошибок и предупреждений

## Миграционное руководство

### Переход с версии 1.0.x на 1.1.x

#### Breaking Changes

1. **Конфигурация**:
   - Замените `TestitemConfig.from_file()` на `load_testitem_config()`
   - Обновите структуру YAML конфигурации (см. раздел "Конфигурация")
   - Переместите `rate_limit` из `http.global` в `sources.{source_name}`

2. **API**:
   - `run_testitem_etl()` теперь принимает `input_path` вместо `input_data`
   - Удалены модули `extract.py`, `normalize.py`, `validate.py`, `persist.py`

3. **Структура проекта**:
   - Консолидация ETL логики в `pipeline.py`
   - Новые Pydantic модели конфигурации

#### Шаги миграции

1. **Обновите импорты**:
   ```python
   # Старый способ
   from library.testitem.config import TestitemConfig
   
   # Новый способ
   from library.testitem.config import load_testitem_config
   ```

2. **Обновите загрузку конфигурации**:
   ```python
   # Старый способ
   config = TestitemConfig.from_file("config.yaml")
   
   # Новый способ
   config = load_testitem_config("config.yaml")
   ```

3. **Обновите вызовы ETL**:
   ```python
   # Старый способ
   result = run_testitem_etl(config, input_data=dataframe)
   
   # Новый способ
   result = run_testitem_etl(config, input_path=Path("input.csv"))
   ```

4. **Обновите конфигурационные файлы**:
   - Переместите `rate_limit` из `http.global` в `sources.{source_name}`
   - Добавьте секции `postprocess` и `logging`
   - Уберите подсекции `csv`/`parquet` из `io.output`

#### Backward Compatibility

Старые функции помечены как deprecated, но продолжают работать:
- `TestitemConfig.from_file()` → `load_testitem_config()`
- `run_testitem_etl(config, input_data=...)` → `run_testitem_etl(config, input_path=...)`

#### Поддержка

При возникновении проблем с миграцией:
1. Проверьте логи на наличие deprecated warnings
2. Обновите код согласно новому API
3. Обратитесь к разделу "Программный интерфейс" для примеров

