# Запуск ETL локально

Руководство по локальному запуску ETL пайплайнов для разработки и тестирования.

## Быстрый старт

### 1. Установка зависимостей

```bash
# Создание виртуального окружения
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# Установка пакета
pip install .[dev]
```

### 2. Базовый запуск

```bash
# Запуск пайплайна Documents
make run ENTITY=documents CONFIG=configs/config_documents_full.yaml

# Или через CLI
bioactivity-data-acquisition pipeline --config configs/config_documents_full.yaml
```

## Доступные пайплайны

### Documents Pipeline

```bash
# Полный пайплайн документов
make run ENTITY=documents CONFIG=configs/config_documents_full.yaml

# Тестовый запуск с ограниченными данными
make run ENTITY=documents CONFIG=configs/config_test.yaml
```

### Targets Pipeline

```bash
# Пайплайн мишеней
make run ENTITY=targets CONFIG=configs/config_target_full.yaml
```

### Assays Pipeline

```bash
# Пайплайн ассеев
make run ENTITY=assays CONFIG=configs/config_assay_full.yaml
```

### Activities Pipeline

```bash
# Пайплайн активностей
make run ENTITY=activities CONFIG=configs/config_activity_full.yaml
```

### Testitems Pipeline

```bash
# Пайплайн тестовых соединений
make run ENTITY=testitems CONFIG=configs/config_testitem_full.yaml
```

## Конфигурация для локальной разработки

### Создание локального конфига

```yaml
# configs/config_local.yaml
sources:
  chembl:
    timeout_sec: 30.0
    max_retries: 3
  crossref:
    timeout_sec: 15.0
    max_retries: 2

output:
  base_dir: "data/output_local"
  create_timestamped_dirs: true

logging:
  level: DEBUG
  console_output: true
```

### Переменные окружения

```bash
# .env файл для локальной разработки
LOG_LEVEL=DEBUG
OUTPUT_DIR=data/output_local
CACHE_DIR=data/cache_local
```

## Режимы запуска

### 1. Полный пайплайн

```bash
# Запуск с полной обработкой данных
make run ENTITY=documents CONFIG=configs/config_documents_full.yaml
```

### 2. Тестовый режим

```bash
# Ограниченный набор данных для быстрого тестирования
make run ENTITY=documents CONFIG=configs/config_test.yaml
```

### 3. Отладочный режим

```bash
# С детальным логированием
LOG_LEVEL=DEBUG make run ENTITY=documents CONFIG=configs/config_documents_full.yaml
```

### 4. Режим без кэширования

```bash
# Принудительное обновление всех данных
CACHE_ENABLED=false make run ENTITY=documents CONFIG=configs/config_documents_full.yaml
```

## Структура выходных данных

После выполнения пайплайна в директории `data/output/` создаются:

```text
data/output/
├── documents_20241201/
│   ├── documents_20241201.csv          # Основные данные
│   ├── documents_20241201_meta.yaml    # Метаданные пайплайна
│   ├── documents_20241201_qc.csv       # QC отчёт
│   └── documents_correlation_report_20241201/  # Корреляционный анализ
│       ├── correlation_matrix.csv
│       ├── source_comparison.csv
│       └── quality_metrics.csv
```

## Мониторинг выполнения

### Логирование

```bash
# Просмотр логов в реальном времени
tail -f logs/app.log

# Фильтрация по уровню
grep "ERROR" logs/app.log
grep "WARNING" logs/app.log
```

### Прогресс-бар

CLI автоматически показывает прогресс выполнения:

```text
Documents Pipeline: 45%|████▌     | 450/1000 [02:15<02:45, 3.33it/s]
```

### Метрики производительности

```bash
# Просмотр метрик после выполнения
cat data/output/documents_*/documents_*_meta.yaml
```

## Отладка проблем

### 1. Проверка конфигурации

```bash
# Валидация конфигурационного файла
bioactivity-data-acquisition validate-config configs/config_documents_full.yaml
```

### 2. Тест подключений к API

```bash
# Проверка доступности всех API
make test-api-connections
```

### 3. Проверка входных данных

```bash
# Валидация входных CSV файлов
python -c "
import pandas as pd
from library.schemas.document_input_schema import DocumentInputSchema

df = pd.read_csv('data/input/documents.csv')
try:
    validated = DocumentInputSchema.validate(df)
    print('✅ Входные данные корректны')
except Exception as e:
    print(f'❌ Ошибка валидации: {e}')
"
```

### 4. Проверка зависимостей

```bash
# Проверка установленных пакетов
pip list | grep -E "(pandas|pandera|requests|typer)"

# Проверка версий
python -c "
import pandas as pd
import pandera as pa
print(f'Pandas: {pd.__version__}')
print(f'Pandera: {pa.__version__}')
"
```

## Оптимизация производительности

### 1. Параллельная обработка

```yaml
# configs/config_optimized.yaml
processing:
  max_workers: 4
  batch_size: 1000
  chunk_size: 5000
```

### 2. Кэширование

```yaml
cache:
  enabled: true
  ttl_hours: 24
  max_size_mb: 1024
```

### 3. Ограничение данных

```yaml
# Для быстрого тестирования
limits:
  max_records: 1000
  max_api_calls: 100
```

## Интеграция с IDE

### VS Code

Создайте `.vscode/launch.json`:

```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Run Documents Pipeline",
            "type": "python",
            "request": "launch",
            "program": "${workspaceFolder}/src/library/cli/__init__.py",
            "args": ["pipeline", "--config", "configs/config_documents_full.yaml"],
            "console": "integratedTerminal",
            "cwd": "${workspaceFolder}"
        }
    ]
}
```

### PyCharm

1. Создайте Run Configuration
2. Script path: `src/library/cli/__init__.py`
3. Parameters: `pipeline --config configs/config_documents_full.yaml`
4. Working directory: корень проекта

## Автоматизация

### Makefile команды

```bash
# Полный набор команд для разработки
make help                    # Список всех команд
make install                 # Установка зависимостей
make test                    # Запуск тестов
make lint                    # Проверка кода
make format                  # Форматирование кода
make docs                    # Сборка документации
make clean                   # Очистка временных файлов
```

### Pre-commit hooks

```bash
# Установка pre-commit hooks
pre-commit install

# Запуск на всех файлах
pre-commit run --all-files
```

## Troubleshooting

### Частые проблемы

1. **ModuleNotFoundError**
   ```bash
   # Убедитесь что пакет установлен в development режиме
   pip install -e .[dev]
   ```

2. **Permission denied**
   ```bash
   # Проверьте права на запись в директории
   chmod -R 755 data/
   ```

3. **Memory issues**
   ```bash
   # Уменьшите batch_size в конфигурации
   processing:
     batch_size: 100
   ```

4. **API rate limits**
   ```bash
   # Увеличьте задержки между запросами
   sources:
     chembl:
       rate_limit:
         requests_per_second: 0.1
   ```

### Получение помощи

```bash
# Справка по CLI
bioactivity-data-acquisition --help
bioactivity-data-acquisition pipeline --help

# Логи с максимальной детализацией
LOG_LEVEL=DEBUG bioactivity-data-acquisition pipeline --config configs/config_documents_full.yaml
```
