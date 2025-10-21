# Makefiles

Документация по всем Makefile'ам в проекте и стандартизованному контракту целей.

## Философия использования Make

В проекте Make используется для:

- **Стандартизации команд**: Единообразные цели для всех пайплайнов
- **Автоматизации**: Упрощение повторяющихся операций
- **Документации**: Справка по доступным командам
- **Кроссплатформенности**: Работа на Windows, Linux, macOS

## Главный Makefile

### Общие цели

```bash
# Показать справку
make help

# Установка и настройка
make setup-api-keys    # Установить API ключи в переменные окружения
make install-dev       # Установить в режиме разработки

# Очистка
make clean-backups     # Очистить backup файлы
make clean             # Очистить все временные файлы

# Тестирование и качество
make test              # Запустить тесты
make health-check      # Проверить здоровье API
make format            # Форматировать код
make lint              # Проверить код линтером
make type-check        # Проверить типы
make quality           # Полная проверка качества кода

# Запуск пайплайнов
make run-dev           # Запустить с тестовыми данными (3 записи)
make run-full          # Запустить с полными данными (100 записей)

# Документация
make docs-serve        # Запустить локальный сервер MkDocs
make docs-build        # Собрать статическую документацию
make docs-lint         # Проверить документацию линтерами
make docs-deploy       # Деплой документации на GitHub Pages

# Комбинированные команды
make quick-start       # Быстрый старт - установить ключи и запустить тест
make full-setup        # Полная настройка и запуск
```

### Детальное описание целей

#### Установка и настройка

```bash
# setup-api-keys
# Устанавливает API ключи в переменные окружения
# Поддерживает Windows (PowerShell) и Unix-системы
make setup-api-keys

# install-dev
# Устанавливает пакет в режиме разработки с dev зависимостями
make install-dev
```

#### Очистка

```bash
# clean-backups
# Удаляет backup файлы из data/output/full/
make clean-backups

# clean
# Удаляет все временные файлы: __pycache__, .pytest_cache, .mypy_cache
make clean
```

#### Качество кода

```bash
# format
# Форматирует код с помощью black и ruff
make format

# lint
# Проверяет код с помощью ruff
make lint

# type-check
# Проверяет типы с помощью mypy
make type-check

# quality
# Выполняет полную проверку: format + lint + type-check
make quality
```

#### Документация

```bash
# docs-serve
# Запускает локальный сервер MkDocs на http://127.0.0.1:8000
make docs-serve

# docs-build
# Собирает статическую документацию в директорию site/
make docs-build

# docs-lint
# Проверяет документацию с помощью markdownlint и pymarkdown
make docs-lint

# docs-deploy
# Деплоит документацию на GitHub Pages
make docs-deploy
```

## Makefile.assay

### Специфичные цели для assay пайплайна

```bash
# Установка и настройка
make -f Makefile.assay install-assay          # Установить зависимости
make -f Makefile.assay validate-assay-config  # Проверить конфигурацию
make -f Makefile.assay check-assay-deps       # Проверить зависимости

# Запуск пайплайна
make -f Makefile.assay assay-example          # Запуск с примером данных
make -f Makefile.assay assay-target           # Извлечение по таргету CHEMBL231
make -f Makefile.assay assay-target-binding   # Извлечь только binding ассеи
make -f Makefile.assay assay-target-functional # Извлечь только functional ассеи
make -f Makefile.assay assay-high-quality     # Извлечь только высококачественные ассеи
make -f Makefile.assay assay-dry-run          # Тестовый запуск без сохранения

# Тестирование
make -f Makefile.assay test-assay             # Запустить тесты
make -f Makefile.assay test-assay-coverage    # Запустить тесты с покрытием
make -f Makefile.assay test-assay-unit        # Запустить только unit тесты
make -f Makefile.assay test-assay-client      # Запустить тесты клиента

# Качество кода
make -f Makefile.assay lint-assay             # Проверить код
make -f Makefile.assay format-assay           # Форматировать код

# Очистка
make -f Makefile.assay clean-assay            # Очистить выходные файлы
make -f Makefile.assay clean-assay-cache      # Очистить только кэш

# Информация
make -f Makefile.assay show-assay-config      # Показать конфигурацию
make -f Makefile.assay show-assay-help        # Показать справку CLI
make -f Makefile.assay show-assay-examples    # Показать примеры использования
make -f Makefile.assay assay-info             # Показать информацию о пайплайне
make -f Makefile.assay assay-status           # Показать статус ChEMBL API

# Комбинированные команды
make -f Makefile.assay assay-full-test        # Полный тест пайплайна
make -f Makefile.assay assay-quick-start      # Быстрый старт
```

### Примеры использования

```bash
# Быстрый старт
make -f Makefile.assay assay-quick-start

# Извлечение высококачественных ассев для таргета
make -f Makefile.assay assay-high-quality

# Тестирование с покрытием кода
make -f Makefile.assay test-assay-coverage

# Полная очистка и тест
make -f Makefile.assay clean-assay test-assay assay-dry-run
```

## Makefile.target

### Специфичные цели для target пайплайна

```bash
# Создание данных
make -f Makefile.target create-target-example # Создать пример входных данных

# Запуск пайплайна
make -f Makefile.target target-example        # Запуск с примерными данными
make -f Makefile.target target-dry-run        # Запуск в режиме dry-run
make -f Makefile.target target-dev            # Запуск в dev режиме
make -f Makefile.target target-limited        # Запуск с ограничением
make -f Makefile.target target-typer          # Запуск через Typer CLI

# Тестирование
make -f Makefile.target test-target           # Запустить тесты

# Валидация
make -f Makefile.target validate-target-config # Валидировать конфигурацию
make -f Makefile.target check-target-deps     # Проверить зависимости

# Очистка
make -f Makefile.target clean-target          # Очистить выходные файлы

# Информация
make -f Makefile.target target-stats          # Показать статистику
make -f Makefile.target target-config-help    # Показать справку по конфигурации
make -f Makefile.target target-examples       # Показать примеры использования
```

### Примеры использования

```bash
# Создание примера и запуск
make -f Makefile.target target-example

# Запуск с ограничением
make -f Makefile.target target-limited LIMIT=50

# Dev режим для тестирования
make -f Makefile.target target-dev

# Через Typer CLI
make -f Makefile.target target-typer
```

## Стандартный контракт целей

### Обязательные цели

Каждый Makefile должен поддерживать следующие цели:

#### install
```bash
make install
# Установка зависимостей для пайплайна
```

#### validate-config
```bash
make validate-config
# Проверка конфигурации пайплайна
```

#### example
```bash
make example
# Запуск пайплайна с примером данных
```

#### dry-run
```bash
make dry-run
# Тестовый запуск без записи файлов
```

#### test
```bash
make test
# Запуск тестов пайплайна
```

#### clean
```bash
make clean
# Очистка артефактов пайплайна
```

#### help
```bash
make help
# Справка по доступным целям
```

### Рекомендуемые цели

#### check-deps
```bash
make check-deps
# Проверка зависимостей пайплайна
```

#### info
```bash
make info
# Информация о пайплайне
```

#### status
```bash
make status
# Статус внешних API
```

## Таблица всех целей

| Makefile | Цель | Описание | Зависимости |
|----------|------|----------|-------------|
| **Makefile** | `help` | Показать справку | - |
| **Makefile** | `setup-api-keys` | Установить API ключи | scripts/setup_api_keys.* |
| **Makefile** | `install-dev` | Установить в dev режиме | pyproject.toml |
| **Makefile** | `test` | Запустить тесты | pytest |
| **Makefile** | `run-dev` | Запуск с тестовыми данными | bioactivity-data-acquisition |
| **Makefile** | `run-full` | Запуск с полными данными | bioactivity-data-acquisition |
| **Makefile** | `health-check` | Проверить здоровье API | bioactivity-data-acquisition |
| **Makefile** | `format` | Форматировать код | black, ruff |
| **Makefile** | `lint` | Проверить код линтером | ruff |
| **Makefile** | `type-check` | Проверить типы | mypy |
| **Makefile** | `quality` | Полная проверка качества | format, lint, type-check |
| **Makefile** | `clean` | Очистить временные файлы | - |
| **Makefile** | `docs-serve` | Запустить MkDocs сервер | mkdocs |
| **Makefile** | `docs-build` | Собрать документацию | mkdocs |
| **Makefile** | `docs-lint` | Проверить документацию | markdownlint, pymarkdown |
| **Makefile** | `docs-deploy` | Деплой документации | mkdocs |
| **Makefile.assay** | `install-assay` | Установить зависимости assay | requirements.txt |
| **Makefile.assay** | `assay-example` | Запуск с примером данных | get_assay_data.py |
| **Makefile.assay** | `assay-target` | Извлечение по таргету | get_assay_data.py |
| **Makefile.assay** | `assay-dry-run` | Тестовый запуск | get_assay_data.py |
| **Makefile.assay** | `test-assay` | Запустить тесты assay | pytest |
| **Makefile.assay** | `clean-assay` | Очистить файлы assay | - |
| **Makefile.assay** | `validate-assay-config` | Проверить конфигурацию | library.assay |
| **Makefile.assay** | `check-assay-deps` | Проверить зависимости | library.assay |
| **Makefile.target** | `create-target-example` | Создать пример данных | - |
| **Makefile.target** | `target-example` | Запуск с примером | get_target_data.py |
| **Makefile.target** | `target-dry-run` | Тестовый запуск | get_target_data.py |
| **Makefile.target** | `target-dev` | Dev режим | get_target_data.py |
| **Makefile.target** | `test-target` | Запустить тесты target | pytest |
| **Makefile.target** | `clean-target` | Очистить файлы target | - |
| **Makefile.target** | `validate-target-config` | Проверить конфигурацию | library.target |
| **Makefile.target** | `check-target-deps` | Проверить зависимости | library.target |

## Примеры использования

### Быстрый старт проекта

```bash
# 1. Клонирование и установка
git clone https://github.com/SatoryKono/bioactivity_data_acquisition.git
cd bioactivity_data_acquisition
make install-dev

# 2. Настройка API ключей
make setup-api-keys

# 3. Быстрый тест
make quick-start
```

### Работа с assay пайплайном

```bash
# 1. Установка зависимостей
make -f Makefile.assay install-assay

# 2. Проверка конфигурации
make -f Makefile.assay validate-assay-config

# 3. Запуск с примером
make -f Makefile.assay assay-example

# 4. Извлечение высококачественных ассев
make -f Makefile.assay assay-high-quality

# 5. Тестирование
make -f Makefile.assay test-assay
```

### Работа с target пайплайном

```bash
# 1. Создание примера данных
make -f Makefile.target create-target-example

# 2. Запуск пайплайна
make -f Makefile.target target-example

# 3. Dev режим для тестирования
make -f Makefile.target target-dev

# 4. Очистка
make -f Makefile.target clean-target
```

### Разработка и тестирование

```bash
# 1. Полная проверка качества
make quality

# 2. Запуск тестов
make test

# 3. Проверка здоровья API
make health-check

# 4. Очистка
make clean
```

### Документация

```bash
# 1. Локальный просмотр документации
make docs-serve

# 2. Проверка документации
make docs-lint

# 3. Сборка документации
make docs-build

# 4. Деплой документации
make docs-deploy
```

## Troubleshooting

### Частые проблемы

1. **"make: command not found"**
   ```bash
   # Windows: Установите Make через Chocolatey или WSL
   choco install make
   
   # Linux: Установите make
   sudo apt-get install make
   
   # macOS: Установите Xcode Command Line Tools
   xcode-select --install
   ```

2. **"No rule to make target"**
   ```bash
   # Проверьте доступные цели
   make help
   make -f Makefile.assay help
   make -f Makefile.target help
   ```

3. **Ошибки в Windows PowerShell**
   ```bash
   # Используйте cmd или WSL
   cmd /c "make help"
   ```

4. **Проблемы с путями**
   ```bash
   # Убедитесь, что находитесь в корне проекта
   pwd
   ls -la Makefile*
   ```

### Отладка

```bash
# Подробный вывод команд
make -n target-example

# Проверка переменных
make -p | grep VARIABLE

# Отладка конкретной цели
make -d target-example
```

## Расширение Makefile'ов

### Добавление новой цели

```makefile
# В Makefile.assay
new-target: ## Описание новой цели
	@echo "Выполнение новой цели..."
	$(PYTHON) $(SCRIPT_DIR)/new_script.py \
		--config $(CONFIG_DIR)/config_assay_full.yaml \
		--output-dir $(OUTPUT_DIR)
	@echo "Новая цель завершена"
```

### Создание нового Makefile

```makefile
# Makefile.newpipeline
.PHONY: help newpipeline-example newpipeline-test

PYTHON := python
SCRIPT_DIR := src/scripts
CONFIG_DIR := configs
OUTPUT_DIR := data/output/newpipeline

help: ## Показать справку
	@echo "NewPipeline Commands:"
	@echo "  newpipeline-example  - Запуск с примером данных"
	@echo "  newpipeline-test     - Запуск тестов"

newpipeline-example: ## Запуск с примером данных
	@echo "Запуск newpipeline с примером данных..."
	$(PYTHON) $(SCRIPT_DIR)/get_newpipeline_data.py \
		--config $(CONFIG_DIR)/config_newpipeline_full.yaml \
		--output-dir $(OUTPUT_DIR)

newpipeline-test: ## Запуск тестов
	@echo "Запуск тестов newpipeline..."
	pytest tests/test_newpipeline_pipeline.py -v
```

### Интеграция с CI/CD

```yaml
# .github/workflows/test.yml
- name: Run tests
  run: make test

- name: Check code quality
  run: make quality

- name: Build documentation
  run: make docs-build
```
