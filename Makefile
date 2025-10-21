# Makefile для bioactivity-data-acquisition

.PHONY: help setup-api-keys clean-backups test run-dev install-dev

# Показать справку
help:
	@echo "Доступные команды:"
	@echo "  setup-api-keys    - Установить API ключи в переменные окружения"
	@echo "  clean-backups     - Очистить backup файлы"
	@echo "  test             - Запустить тесты"
	@echo "  run-dev          - Запустить с тестовыми данными (3 записи)"
	@echo "  run-full         - Запустить с полными данными (100 записей)"
	@echo "  install-dev      - Установить в режиме разработки"
	@echo "  health-check     - Проверить здоровье API"
	@echo "  format           - Форматировать код"
	@echo "  lint             - Проверить код линтером"
	@echo "  type-check       - Проверить типы"

# Установить API ключи
setup-api-keys:
	@echo "🔑 Установка API ключей..."
ifeq ($(OS),Windows_NT)
	powershell -ExecutionPolicy Bypass -File scripts/setup_api_keys.ps1
else
	python scripts/setup_api_keys.py
endif

# Очистить backup файлы
clean-backups:
	@echo "🧹 Очистка backup файлов..."
ifeq ($(OS),Windows_NT)
	powershell -Command "Remove-Item 'data\output\full\*.backup' -Force -ErrorAction SilentlyContinue"
else
	find data/output/full -name "*.backup" -delete 2>/dev/null || true
endif

# Запустить тесты
test:
	@echo "🧪 Запуск тестов..."
	pytest tests/ -v

# Запустить с тестовыми данными
run-dev:
	@echo "🚀 Запуск с тестовыми данными (3 записи)..."
	bioactivity-data-acquisition get-document-data --config configs/config_documents_full.yaml --limit 3

# Запустить с полными данными
run-full:
	@echo "🚀 Запуск с полными данными (100 записей)..."
	bioactivity-data-acquisition get-document-data --config configs/config_documents_full.yaml --limit 100

# Установить в режиме разработки
install-dev:
	@echo "📦 Установка в режиме разработки..."
	pip install -e .[dev]

# Проверить здоровье API
health-check:
	@echo "🏥 Проверка здоровья API..."
	bioactivity-data-acquisition health --config configs/config_documents_full.yaml

# Форматировать код
format:
	@echo "🎨 Форматирование кода..."
	black src/ tests/
	ruff check src/ tests/ --fix

# Проверить код линтером
lint:
	@echo "🔍 Проверка кода линтером..."
	ruff check src/ tests/

# Проверить типы
type-check:
	@echo "🔍 Проверка типов..."
	mypy src/

# Полная проверка качества кода
quality: format lint type-check

# Очистить все временные файлы
clean: clean-backups
	@echo "🧹 Очистка временных файлов..."
ifeq ($(OS),Windows_NT)
	powershell -Command "Remove-Item '__pycache__' -Recurse -Force -ErrorAction SilentlyContinue"
	powershell -Command "Remove-Item 'src\**\__pycache__' -Recurse -Force -ErrorAction SilentlyContinue"
	powershell -Command "Remove-Item 'tests\**\__pycache__' -Recurse -Force -ErrorAction SilentlyContinue"
	powershell -Command "Remove-Item '.pytest_cache' -Recurse -Force -ErrorAction SilentlyContinue"
	powershell -Command "Remove-Item '.mypy_cache' -Recurse -Force -ErrorAction SilentlyContinue"
else
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
endif

# Быстрый старт - установить ключи и запустить тест
quick-start: setup-api-keys run-dev

# Полная настройка и запуск
full-setup: install-dev setup-api-keys clean-backups run-dev

# Документация
docs-serve: ## Запустить локальный сервер MkDocs
	@echo "📚 Запуск локального сервера документации..."
	mkdocs serve --config-file configs/mkdocs.yml

docs-build: ## Собрать статическую документацию
	@echo "📚 Сборка документации..."
	mkdocs build --config-file configs/mkdocs.yml --strict

docs-lint: ## Проверить документацию линтерами
	@echo "📚 Проверка документации линтерами..."
	markdownlint docs/ --config .markdownlint.json
	pymarkdown scan docs/

docs-deploy: ## Деплой документации на GitHub Pages (локально)
	@echo "📚 Деплой документации на GitHub Pages..."
	mkdocs gh-deploy --config-file configs/mkdocs.yml --force