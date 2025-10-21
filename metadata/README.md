# Metadata Directory

Эта папка содержит метаданные проекта, организованные по типам и пайплайнам. Все манифесты и отчёты следуют единой схеме именования и структуры.

## Структура

```
metadata/
├── manifests/          # Манифесты пайплайнов и процессов
│   ├── documents_pipeline.json      # Основной манифест пайплайна документов
│   ├── documents_postprocess.json   # Манифест постобработки документов
│   ├── targets_pipeline.json        # Манифест пайплайна целей (targets)
│   ├── assays_pipeline.json         # Манифест пайплайна анализов (assays)
│   ├── activities_pipeline.json     # Манифест пайплайна активностей (activities)
│   ├── testitems_pipeline.json      # Манифест пайплайна тестовых элементов (testitems)
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

### targets_pipeline.json
Манифест пайплайна обработки целей (targets). Содержит:
- Конфигурацию источников (ChEMBL, UniProt, IUPHAR)
- Этапы ETL (extract, normalize, validate, enrich, load)
- Схемы валидации и контроля качества
- CLI команды для извлечения данных целей
- Артефакты и выходные файлы

### assays_pipeline.json
Манифест пайплайна обработки анализов (assays). Содержит:
- Конфигурацию источника ChEMBL
- Поля данных анализов (тип, категория, организм, ткань)
- Схемы валидации и контроля качества
- CLI команды для извлечения данных анализов
- Артефакты и выходные файлы

### activities_pipeline.json
Манифест пайплайна обработки активностей (activities). Содержит:
- Конфигурацию источника ChEMBL
- Поля данных активностей (тип, значение, единицы измерения)
- Схемы валидации и контроля качества
- CLI команды для извлечения данных активностей
- Артефакты и выходные файлы

### testitems_pipeline.json
Манифест пайплайна обработки тестовых элементов (testitems). Содержит:
- Конфигурацию источников (ChEMBL, PubChem)
- Поля молекулярных данных (SMILES, InChI, свойства)
- Схемы валидации структур
- CLI команды для извлечения данных молекул
- Артефакты и выходные файлы

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
Манифесты связаны с CLI командами через поле `cli_commands`:

```bash
# Основной пайплайн документов
python -m library.cli get-document-data --config configs/config_documents_full.yaml

# Пайплайн целей (targets)
python -m library.cli get-target-data --config configs/config_target_full.yaml --input data/input/target.csv

# Пайплайн анализов (assays)
python -m library.cli get-assay-data --config configs/config_assay_full.yaml --input data/input/assay.csv

# Пайплайн активностей (activities)
python -m library.cli get-activity-data --config configs/config_activity_full.yaml --input data/input/activity.csv

# Пайплайн тестовых элементов (testitems)
python -m library.cli testitem-run --config configs/config_testitem_full.yaml --input data/input/testitem.csv

# Проверка здоровья API
python -m library.cli health --config configs/config_documents_full.yaml

# Универсальный пайплайн
python -m library.cli pipeline --config configs/config.yaml
```

### Конфигурационные файлы
Манифесты ссылаются на конфигурационные файлы:
- `configs/config.yaml` - основная конфигурация
- `configs/config_documents_full.yaml` - полная конфигурация документов
- `configs/config_target_full.yaml` - полная конфигурация целей
- `configs/config_assay_full.yaml` - полная конфигурация анализов
- `configs/config_activity_full.yaml` - полная конфигурация активностей
- `configs/config_testitem_full.yaml` - полная конфигурация тестовых элементов
- `configs/schema.json` - JSON схема конфигурации

### Артефакты
Манифесты определяют входные и выходные артефакты:
- **Входные**: `data/input/*.csv` (documents.csv, target.csv, assay.csv, activity.csv, testitem.csv)
- **Выходные по сущностям**:
  - **Documents**: `data/output/documents/*.csv`, `data/output/_documents/*.csv`
  - **Targets**: `data/output/target/*.csv`, `data/output/target/*.yaml`
  - **Assays**: `data/output/assay/*.csv`, `data/output/assay/*.yaml`
  - **Activities**: `data/output/activity/*.csv`, `data/output/activity/*.yaml`
  - **Testitems**: `data/output/testitem/*.csv`, `data/output/testitem/*.yaml`
- **Отчёты QC**: `data/output/*/qc_report_*.csv`
- **Метаданные**: `data/output/*/metadata_*.yaml`

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

Манифесты интегрированы с `pyproject.toml` через секцию `[project.metadata]`:

```toml
[project.metadata]
# Метаданные манифестов и отчётов
manifests = [
    "metadata/manifests/documents_pipeline.json",
    "metadata/manifests/documents_postprocess.json", 
    "metadata/manifests/cleanup_manifest.json",
    "metadata/manifests/quality_manifest.json",
    "metadata/manifests/targets_pipeline.json",
    "metadata/manifests/assays_pipeline.json",
    "metadata/manifests/activities_pipeline.json",
    "metadata/manifests/testitems_pipeline.json"
]
reports = [
    "metadata/reports/config_audit.csv"
]
```

Дополнительная интеграция:
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
