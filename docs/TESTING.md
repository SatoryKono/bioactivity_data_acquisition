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

# Через Makefile

make test-unit

# Напрямую через pytest

pytest tests/unit/ -v

```
### Integration тесты
```bash

# Через Makefile (continued 1)

make test-integration

# Напрямую через pytest (continued 1)

pytest tests/integration/ -v

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
### Только быстрые тесты (unit + smoke)
```bash
make test-unit

# или

pytest tests/unit/ -v

```
## Конфигурация
- `pyproject.toml` — общая конфигурация проекта и инструментов; секция `[tool.pytest.ini_options]` содержит настройки pytest
- `tests/conftest.py` — общие фикстуры и настройки пути
- Интеграционные тесты документного пайплайна используют профиль `configs/profiles/document_test.yaml`,
  в котором отключены внешние API и заданы фиктивные ключи. Благодаря этому `pytest tests/integration/test_document_pipeline_enrichment.py -v`
  выполняется без реальных секретов и сетевого доступа. При необходимости профиль можно загрузить вручную:

  ```bash
  python -c "from bioetl.config import load_config; print(load_config('configs/profiles/document_test.yaml'))"
  ```
## Структура тестов
```text

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
