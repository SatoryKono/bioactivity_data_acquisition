# Инструкции по запуску тестов

## Быстрый старт

### Запуск всех тестов

```bash
# Linux/Mac
./run_tests.sh

# Windows PowerShell
.\run_tests.ps1

# Или напрямую через pytest
python -m pytest tests/ -v
```

## Запуск отдельных групп тестов

### Unit тесты

```bash
pytest tests/unit/ -v -m unit
```

### Integration тесты

```bash
pytest tests/integration/ -v -m integration
```

### Golden и smoke тесты CLI

```bash
pytest tests/golden/ -v -m golden
```

### Конкретный тест

```bash
pytest tests/unit/test_pipelines.py::TestActivityPipeline::test_init -v
```

## Флаги и опции

### С покрытием кода

```bash
pytest tests/ --cov=src/bioetl --cov-report=html
```

### Пропуск медленных тестов

```bash
pytest tests/ -v -m "not slow"
```

### Только быстрые тесты

```bash
pytest tests/ -v -m unit
```

## Конфигурация

- `pytest.ini` - основные настройки pytest
- `pyproject.toml` - общая конфигурация проекта и инструментов
- `tests/conftest.py` - общие фикстуры и настройки пути

## Структура тестов

```
tests/
├── conftest.py                 # Общие фикстуры
├── unit/                       # Юнит-тесты
│   ├── test_pipelines.py       # Тесты пайплайнов
│   ├── test_config_loader.py   # Тесты конфигурации
│   ├── test_api_client.py      # Тесты API клиента
│   ├── test_logger.py          # Тесты логирования
│   ├── test_hashing.py         # Тесты хеширования
│   └── adapters/               # Тесты адаптеров
└── integration/                # Интеграционные тесты
    └── test_document_pipeline_enrichment.py
```

## Требования

- Минимальное покрытие кода: 85%
- Все тесты должны проходить без ошибок
- Использовать маркеры: `unit`, `integration`, `golden`, `slow`
- Golden тесты проверяют детерминированные CSV, полученные через CLI
- Установите зависимость `faker` (через `pip install -e ".[dev]"` или `pip install -r requirements.txt`),
  иначе фикстуры в `tests/conftest.py` завершатся с ошибкой.

## Troubleshooting

### Импорты не работают
Убедитесь, что `src` добавлен в `PYTHONPATH`:

```bash
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"
```

### Тесты падают с ImportError
Переустановите пакет:

```bash
pip install -e ".[dev]"
```

### Низкое покрытие кода
Добавьте больше тестов для недостающих модулей:

```bash
pytest --cov=src/bioetl --cov-report=term-missing
```
