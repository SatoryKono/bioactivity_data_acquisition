# Как добавить новый пайплайн

## Обзор

Руководство по добавлению нового пайплайна в проект Bioactivity Data Acquisition.

## Шаги

### 1. Создание структуры

Создайте следующие файлы:
- `src/library/pipelines/{entity}_pipeline.py`
- `configs/config_{entity}_full.yaml`
- `docs/pipelines/{entity}.md`

### 2. Реализация пайплайна

#### Базовый класс

```python
from library.pipelines.base import BasePipeline

class {Entity}Pipeline(BasePipeline):
    def __init__(self, config: dict):
        super().__init__(config)
        self.entity_name = "{entity}"
    
    def extract(self) -> pd.DataFrame:
        # Реализация извлечения данных
        pass
    
    def normalize(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        # Реализация нормализации
        pass
    
    def validate(self, normalized_data: pd.DataFrame) -> pd.DataFrame:
        # Реализация валидации
        pass
    
    def postprocess(self, validated_data: pd.DataFrame) -> pd.DataFrame:
        # Реализация постобработки
        pass
```

### 3. Конфигурация

Создайте конфигурационный файл по образцу существующих:

```yaml
# configs/config_{entity}_full.yaml
http:
  global:
    timeout_sec: 60.0
    retries:
      total: 10
      backoff_multiplier: 3.0
    headers:
      Accept: application/json
      User-Agent: bioactivity-data-acquisition/0.1.0

sources:
  {source}:
    name: {source}
    enabled: true
    endpoint: {endpoint}
    pagination:
      size: 200
      max_pages: 50
    http:
      base_url: {api_url}
      timeout_sec: 60.0
      headers:
        Accept: application/json

io:
  input:
    {entity}s_csv: data/input/{entity}s.csv
  output:
    data_path: data/output/full/{entity}s.csv
    qc_report_path: data/output/full/{entity}s_qc_report.csv
    format: csv
    csv:
      encoding: utf-8
      float_format: "%.3f"
      date_format: "%Y-%m-%dT%H:%M:%SZ"

runtime:
  workers: 4
  limit: null
  dry_run: false
  date_tag: null

logging:
  level: INFO
  file:
    enabled: true
    path: logs/app.log
    max_bytes: 10485760
    backup_count: 10
    rotation_strategy: size
    retention_days: 14
    cleanup_on_start: false
  console:
    format: text

validation:
  strict: true
  qc:
    max_missing_fraction: 0.05
    max_duplicate_fraction: 0.01

determinism:
  sort:
    by:
      - {primary_key}
    ascending:
      - true
    na_position: last
  column_order:
    - {entity}_id
    - {entity}_name
    - retrieved_at

postprocess:
  qc:
    enabled: true
```

### 4. Документация

Создайте документацию по стандарту 11 блоков:

1. Назначение и границы
2. Источники данных и маппинги
3. Граф ETL (Mermaid)
4. Схемы данных: вход/выход + таблица полей
5. Конфигурация
6. Валидация
7. Детерминизм
8. CLI/Make: команды запуска
9. Артефакты
10. Контроль качества
11. Ограничения и типичные ошибки

### 5. Интеграция

- Добавьте пайплайн в CLI
- Создайте Makefile цели
- Добавьте тесты
- Обновите навигацию в MkDocs

### 6. Тестирование

- Запустите пайплайн с тестовыми данными
- Проверьте выходные данные
- Убедитесь в корректности QC отчёта
- Проверьте документацию

## Примеры

Смотрите существующие пайплайны:
- `targets` - для простых сущностей
- `assays` - для связанных данных
- `testitems` - для сложных маппингов