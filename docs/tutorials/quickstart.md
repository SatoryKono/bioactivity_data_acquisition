# Быстрый старт

Изучите основы работы с Bioactivity Data Acquisition за 15 минут.

## Что вы изучите

В этом туториале вы изучите:
- Установку и настройку системы
- Запуск базового ETL-пайплайна
- Интерпретацию результатов
- Основы конфигурации

## Предпосылки

Перед началом убедитесь, что у вас есть:
- Python 3.11+ установлен
- Базовые знания работы с командной строкой
- Доступ к интернету для загрузки зависимостей

## Шаг 1: Установка

Создайте виртуальное окружение и установите пакет:

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install --upgrade pip
pip install .[dev]
```

**Ожидаемый результат:**
```
Successfully installed bioactivity-data-acquisition-0.1.0
```

### Проверка установки

```bash
bioactivity-data-acquisition --help
```

**Ожидаемый результат:**
```
Usage: bioactivity-data-acquisition [OPTIONS] COMMAND [ARGS]...

  Bioactivity Data Acquisition CLI

Commands:
  get-document-data  Enrich documents with data from multiple API sources
  install-completion Install shell completion
  pipeline          Run the main ETL pipeline
  version           Show version information
```

## Шаг 2: Первый запуск

Запустите пайплайн с базовой конфигурацией:

```bash
bioactivity-data-acquisition pipeline --config configs/config.yaml
```

**Ожидаемый результат:**
```
INFO: Starting ETL pipeline
INFO: Extracting data from chembl
INFO: Transforming data
INFO: Loading data to data/output/bioactivities.csv
INFO: Generating QC report
INFO: Pipeline completed successfully
```

### Что происходит

Система выполняет полный ETL-цикл:
1. **Extract** — извлекает данные из ChEMBL API
2. **Transform** — нормализует и валидирует данные
3. **Load** — сохраняет результаты в CSV файлы

## Шаг 3: Проверка результатов

Проверьте созданные файлы:

```bash
ls -la data/output/
```

**Ожидаемый результат:**
```
-rw-r--r-- 1 user user 12345 Jan 17 10:30 bioactivities.csv
-rw-r--r-- 1 user user  1234 Jan 17 10:30 bioactivities_qc_report.csv
-rw-r--r-- 1 user user   567 Jan 17 10:30 bioactivities_correlation.csv
```

### Изучение данных

```bash
head -5 data/output/bioactivities.csv
```

**Ожидаемый результат:**
```
molecule_chembl_id,assay_chembl_id,activity_value,activity_units,activity_type
CHEMBL123,ASSAY456,10.5,IC50,IC50
CHEMBL789,ASSAY012,8.2,IC50,IC50
```

## Шаг 4: Настройка конфигурации

Создайте копию конфигурации для экспериментов:

```bash
cp configs/config.yaml configs/my_config.yaml
```

Отредактируйте `configs/my_config.yaml`:

```yaml
runtime:
  workers: 2  # Уменьшить количество воркеров
  limit: 100  # Ограничить количество записей

logging:
  level: DEBUG  # Включить детальное логирование
```

Запустите с новой конфигурацией:

```bash
bioactivity-data-acquisition pipeline --config configs/my_config.yaml
```

## Шаг 5: Переопределение параметров

Используйте флаг `--set` для переопределения параметров:

```bash
bioactivity-data-acquisition pipeline \
  --config configs/config.yaml \
  --set runtime.workers=1 \
  --set logging.level=DEBUG
```

## Проверка результата

Убедитесь, что пайплайн работает стабильно:

```bash
# Запустите дважды с одинаковой конфигурацией
bioactivity-data-acquisition pipeline --config configs/config.yaml
cp data/output/bioactivities.csv run1.csv

bioactivity-data-acquisition pipeline --config configs/config.yaml
cp data/output/bioactivities.csv run2.csv

# Сравните результаты
diff run1.csv run2.csv
```

**Ожидаемый результат:**
```
# Файлы должны быть идентичными (детерминизм)
```

## Следующие шаги

Теперь, когда вы изучили основы, вы можете:

- [Полный пайплайн (E2E)](e2e-pipeline.md) — углубиться в детали ETL
- [Обогащение документов (E2E)](e2e-documents.md) — изучить работу с документами
- [Настройка API клиентов](../how-to/configure-api-clients.md) — настроить внешние источники
- [Отладка пайплайна](../how-to/debug-pipeline.md) — решать проблемы

## Troubleshooting

### Проблема: Ошибка "ModuleNotFoundError"

**Решение:**
Убедитесь, что виртуальное окружение активировано:
```bash
source .venv/bin/activate  # Linux/macOS
.venv\Scripts\activate     # Windows
```

### Проблема: Ошибка "Permission denied"

**Решение:**
Проверьте права доступа к директории:
```bash
chmod 755 data/output/
```

### Проблема: Пустые выходные файлы

**Решение:**
Проверьте логи и убедитесь, что API доступен:
```bash
bioactivity-data-acquisition pipeline --config configs/config.yaml --set logging.level=DEBUG
```

## Связанные темы

- [Установка](../how-to/installation.md)
- [Конфигурация](../reference/configuration/index.md)
- [CLI команды](../reference/cli/index.md)
- [Архитектура](../explanations/architecture.md)
