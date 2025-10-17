# CI/CD

Основной конвейер: `.github/workflows/ci.yaml`

## Триггеры

- `push` в ветки `main`, `develop`
- `pull_request` в ветки `main`, `develop`

## Проверки качества

Выполняются шаги для каждой версии Python (3.10, 3.11, 3.12):

1. checkout
2. setup-python (матрица версий)
3. cache pip dependencies
4. install deps: `pip install -e ".[dev]"`
5. lint with ruff: `ruff check src/ tests/` и `ruff format --check src/ tests/`
6. type check with mypy: `mypy src/`
7. security check with bandit: `bandit -r src/ -f json -o bandit-report.json`
8. security check with safety: `safety check --json --output safety-report.json`
9. test with pytest: `pytest --cov=library --cov=tests --cov-report=xml --cov-report=html --cov-report=term-missing`
10. upload coverage to Codecov

Порог покрытия: задаётся в `pyproject.toml` → `[tool.pytest.ini_options] addopts` с `--cov-fail-under=90`.

## Артефакты CI

Все сгенерированные файлы загружаются как артефакты GitHub Actions вместо коммита в репозиторий:

### Тестовые выходы

- **Имя артефакта**: `test-outputs-{python-version}`
- **Путь**: `tests/test_outputs/**`
- **Содержимое**: Сгенерированные тестовые файлы

### Отчеты покрытия

- **Имя артефакта**: `coverage-{python-version}`
- **Пути**:
  - `coverage.xml` - XML отчет для Codecov
  - `htmlcov/**` - HTML отчеты покрытия
- **Содержимое**: Отчеты о покрытии кода тестами

### Отчеты безопасности

- **Имя артефакта**: `security-reports-{python-version}`
- **Пути**:
  - `bandit-report.json` - Отчет Bandit о проблемах безопасности
  - `safety-report.json` - Отчет Safety о уязвимых зависимостях
- **Содержимое**: JSON отчеты о проверках безопасности

### Результаты бенчмарков

- **Имя артефакта**: `benchmark-results-{python-version}`
- **Путь**: `benchmark_results.json`
- **Содержимое**: Результаты тестов производительности

### Логи

- **Имя артефакта**: `test-logs-{python-version}`
- **Путь**: `logs/`
- **Содержимое**: Логи выполнения тестов

## Доступ к артефактам

1. Перейдите на страницу Actions в GitHub
2. Выберите нужный workflow run
3. В разделе "Artifacts" скачайте нужные артефакты
4. Артефакты доступны в течение 90 дней

## Документация

Workflow документации: `.github/workflows/docs.yml`

- **Триггеры**: push/PR в `main`, изменения в `docs/**`, `configs/mkdocs.yml`
- **Артефакты**: `docs-preview` (только для PR) - предварительный просмотр документации
- **Деплой**: Автоматический деплой в GitHub Pages для ветки `main`
