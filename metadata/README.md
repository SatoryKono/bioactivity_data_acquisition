# Metadata Directory

Эта папка содержит метаданные проекта, организованные по типам и пайплайнам. Все манифесты и отчёты следуют единой схеме именования и структуры.

## Структура

```
metadata/
├── manifests/          # Манифесты пайплайнов и процессов
│   ├── documents_pipeline.json      # Основной манифест пайплайна документов
│   ├── documents_postprocess.json   # Манифест постобработки документов
│   ├── cleanup_manifest.json        # Манифест процесса очистки
│   └── quality_manifest.json        # Манифест контроля качества
└── reports/            # Отчёты и аудиты
    └── config_audit.csv             # Аудит конфигурации
```

## Манифесты (manifests/)

Манифесты содержат метаданные о различных процессах и пайплайнах в формате JSON:

### documents_pipeline.json
Основной манифест пайплайна обработки документов. Содержит:
- Информацию о репозитории и версиях
- Конфигурацию CLI и конфигов
- Этапы ETL (Extract, Validate, Transform, Load)
- Зависимости runtime и dev
- CI/CD настройки
- Артефакты

### documents_postprocess.json
Детальный манифест постобработки документов. Содержит:
- Модуль и исходные таблицы
- Пошаговые этапы постобработки (P01-P06)
- Промежуточные и финальные артефакты
- Настройки детерминизма (сортировка, порядок колонок, NA policy)
- CLI команды и опции
- Настройки логирования

### cleanup_manifest.json
Манифест процесса очистки данных. Содержит:
- Список больших файлов с размерами
- Логи и временные файлы
- Тестовые выходы
- Кэш Python (__pycache__)

### quality_manifest.json
Манифест контроля качества. Содержит:
- Анализ версий Python и пакетных менеджеров
- CLI фреймворк и команды
- Конфигурационные файлы и форматы
- Схемы данных и конфигурации
- CI/CD настройки и проверки
- Инструменты разработки
- Настройки детерминизма
- Обсервабельность (логирование, трейсинг)
- Docker конфигурацию
- Документацию
- Quality gates
- Известные проблемы

## Отчёты (reports/)

Отчёты содержат результаты аудитов и анализа в формате CSV/TSV:

### config_audit.csv
Аудит конфигурационных параметров с колонками:
- `parameter` - имя параметра
- `value` - значение
- `source` - источник (файл, переменная окружения)
- `status` - статус (valid, invalid, missing)
- `recommendation` - рекомендации

## Схема именования

### Манифесты
Формат: `<pipeline>_<process>.json`

Примеры:
- `documents_pipeline.json` - основной манифест пайплайна документов
- `documents_postprocess.json` - манифест постобработки документов
- `cleanup_manifest.json` - манифест очистки
- `quality_manifest.json` - манифест качества

### Отчёты
Формат: `<type>_<pipeline>/<filename>`

Примеры:
- `config_audit.csv` - аудит конфигурации
- `quality_documents/validation_report.csv` - отчёт валидации документов
- `performance_assays/benchmark_results.csv` - результаты бенчмарков

## Связь с пайплайнами

### CLI команды
Манифесты связаны с CLI командами через поле `cli.command`:

```bash
# Основной пайплайн документов
python -m library.cli get-document-data --config configs/config_documents_full.yaml

# Постобработка документов
python -m library.cli etl documents postprocess --config configs/config_documents_full.yaml

# Очистка данных
python -m library.cli cleanup --manifest metadata/manifests/cleanup_manifest.json

# Контроль качества
python -m library.cli quality-check --manifest metadata/manifests/quality_manifest.json
```

### Конфигурационные файлы
Манифесты ссылаются на конфигурационные файлы:
- `configs/config.yaml` - основная конфигурация
- `configs/config_documents_full.yaml` - полная конфигурация документов
- `configs/schema.json` - JSON схема конфигурации

### Артефакты
Манифесты определяют входные и выходные артефакты:
- **Входные**: `data/input/*.csv`
- **Промежуточные**: `data/output/post/*.parquet`
- **Финальные**: `data/output/out/*.csv`, `data/output/out/*.parquet`
- **Отчёты**: `data/output/out/*_report.csv`

## Формат JSON

Все манифесты следуют единому формату JSON с обязательными полями:

```json
{
  "repo": "SatoryKono/bioactivity_data_acquisition",
  "module": "documents",
  "cli": {
    "command": "etl documents postprocess",
    "options": ["--config PATH", "--output PATH"]
  },
  "artifacts": {
    "intermediate": ["post/01_keys.parquet"],
    "final": ["out/documents.csv"]
  },
  "determinism": {
    "sorted_by": ["document_chembl_id"],
    "column_order": ["index", "document_chembl_id"]
  }
}
```

## Интеграция с pyproject.toml

Манифесты интегрированы с `pyproject.toml` через:
- Зависимости в секции `[project.dependencies]`
- CLI entry points в секции `[project.scripts]`
- Конфигурация инструментов в секции `[tool.*]`

## Мониторинг и аудит

Манифесты используются для:
- Автоматической генерации отчётов
- Валидации конфигураций
- Мониторинга качества данных
- Аудита производительности
- Отслеживания изменений в пайплайнах
