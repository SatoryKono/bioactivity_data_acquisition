# CI/CD

Документация CI/CD процессов, pre-commit hooks и релизного процесса.

## Текущее состояние

### GitHub Actions

В проекте настроены следующие workflow:

- **`.github/workflows/docs.yml`** — сборка и деплой документации на GitHub Pages
- **Отсутствуют**: основные CI/CD workflow для тестов, линтеров, безопасности

### Pre-commit hooks

Настроены и работают локально:

- **Code formatting**: black, ruff
- **Type checking**: mypy
- **Security**: проверка секретов
- **Markdown**: markdownlint
- **Custom hooks**: блокировка коммитов в logs/, reports/

## Рекомендуемый пайплайн

### Основной CI/CD workflow

```yaml
# .github/workflows/ci.yml
name: CI

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main, develop ]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: [3.10, 3.11, 3.12]
    
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Python ${{ matrix.python-version }}
        uses: actions/setup-python@v4
        with:
          python-version: ${{ matrix.python-version }}
      
      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install .[dev]
      
      - name: Run tests
        run: |
          pytest tests/ --cov=library --cov-report=xml --cov-report=term-missing
      
      - name: Upload coverage to Codecov
        uses: codecov/codecov-action@v3
        with:
          file: ./coverage.xml
          flags: unittests
          name: codecov-umbrella
```

### Линтеры и форматирование

```yaml
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install .[dev]
      
      - name: Run black
        run: black --check src/ tests/
      
      - name: Run ruff
        run: ruff check src/ tests/
      
      - name: Run mypy
        run: mypy src/
```

### Безопасность

```yaml
  security:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install .[dev]
      
      - name: Run bandit
        run: bandit -r src/ -f json -o bandit-report.json
      
      - name: Run safety
        run: safety check --json --output safety-report.json
      
      - name: Upload security reports
        uses: actions/upload-artifact@v3
        with:
          name: security-reports
          path: |
            bandit-report.json
            safety-report.json
```

### Сборка документации

```yaml
  docs:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install .[dev,docs]
      
      - name: Lint Markdown
        run: |
          markdownlint docs/ --config .markdownlint.json
          pymarkdown scan docs/
      
      - name: Build documentation
        run: |
          mkdocs build --strict
      
      - name: Upload docs artifact
        uses: actions/upload-pages-artifact@v3
        with:
          path: ./site
```

## Тесты

### Структура tests/

```
tests/
├── test_activity_pipeline.py      # Тесты пайплайна активностей
├── test_assay_pipeline.py         # Тесты пайплайна ассев
├── test_target_pipeline.py        # Тесты пайплайна мишеней
├── test_testitem_pipeline.py      # Тесты пайплайна молекул
├── test_documents_pipeline.py     # Тесты пайплайна документов
├── test_clients/                  # Тесты HTTP клиентов
│   ├── test_chembl_client.py
│   ├── test_crossref_client.py
│   ├── test_pubmed_client.py
│   └── test_semantic_scholar_client.py
├── test_schemas/                  # Тесты Pandera схем
│   ├── test_activity_schema.py
│   ├── test_assay_schema.py
│   └── test_target_schema.py
├── test_etl/                      # Тесты ETL утилит
│   ├── test_extract.py
│   ├── test_transform.py
│   └── test_load.py
└── test_data/                     # Тестовые данные
    ├── sample_activity.csv
    ├── sample_assay.csv
    └── sample_target.csv
```

### Маркеры тестов

```python
# pytest.ini
[tool:pytest]
markers =
    benchmark: mark test as a benchmark
    integration: mark test as integration test requiring real API access
    slow: mark test as slow running
    network: mark test as requiring network access
    api: mark test as requiring API access
```

### Запуск тестов

```bash
# Все тесты
pytest tests/

# Только unit тесты
pytest tests/ -m "not integration and not network"

# Только integration тесты
pytest tests/ -m integration

# Тесты с покрытием
pytest tests/ --cov=library --cov-report=html

# Тесты производительности
pytest tests/ -m benchmark

# Параллельный запуск
pytest tests/ -n auto
```

### Типы тестов

1. **Unit тесты** — тестирование отдельных функций и классов
2. **Integration тесты** — тестирование взаимодействия с внешними API
3. **Contract тесты** — проверка контрактов API
4. **E2E тесты** — полный цикл обработки данных
5. **QC тесты** — проверка качества данных
6. **Benchmark тесты** — тестирование производительности

## Pre-commit hooks

### Конфигурация

```yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v4.6.0
    hooks:
      - id: check-added-large-files
        args: ["--maxkb=500"]
      - id: end-of-file-fixer
      - id: trailing-whitespace
      - id: mixed-line-ending
        args: ["--fix=lf"]
      - id: check-yaml
      - id: check-merge-conflict
      - id: check-case-conflict

  - repo: https://github.com/psf/black
    rev: 24.4.2
    hooks:
      - id: black
        language_version: python3

  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.4.4
    hooks:
      - id: ruff
        args: [--fix, --exit-non-zero-on-fix]

  - repo: https://github.com/pre-commit/mirrors-mypy
    rev: v1.8.0
    hooks:
      - id: mypy
        additional_dependencies: [types-PyYAML, pandera]
        args: [--ignore-missing-imports]

  - repo: https://github.com/igorshubovych/markdownlint-cli
    rev: v0.39.0
    hooks:
      - id: markdownlint
        args: [--config, .markdownlint.json]
        files: ^docs/.*\.md$
```

### Установка и использование

```bash
# Установка pre-commit
pip install pre-commit

# Установка hooks
pre-commit install

# Запуск на всех файлах
pre-commit run --all-files

# Запуск конкретного hook
pre-commit run black

# Обновление hooks
pre-commit autoupdate
```

### Кастомные hooks

```yaml
  - repo: local
    hooks:
      - id: block-logs-reports
        name: Block committing logs and reports artifacts
        entry: python -c "
import sys
import os
import re

# Whitelisted files that are allowed in logs/, reports/, and tests/test_outputs/
ALLOWED_PATTERNS = [
    r'logs/README\.md$',
    r'reports/README\.md$', 
    r'logs/\.gitkeep$',
    r'reports/\.gitkeep$',
    r'logs/\.gitignore$',
    r'reports/\.gitignore$',
    r'reports/config_audit\.csv$',
    r'tests/test_outputs/\.gitkeep$',
]

def is_allowed_file(filepath):
    for pattern in ALLOWED_PATTERNS:
        if re.match(pattern, filepath):
            return True
    return False

def main():
    blocked_files = []
    
    for filepath in sys.argv[1:]:
        if (filepath.startswith('logs/') or filepath.startswith('reports/') or filepath.startswith('tests/test_outputs/')) and not is_allowed_file(filepath):
            blocked_files.append(filepath)
    
    if blocked_files:
        print('❌ BLOCKED: Attempting to commit files in logs/, reports/, or tests/test_outputs/ directories:')
        for filepath in blocked_files:
            print(f'   - {filepath}')
        print()
        print('💡 These directories are for generated artifacts and should not be committed.')
        print('   Allowed files: README.md, .gitkeep, .gitignore, and whitelisted data files.')
        print('   Use .gitignore to exclude generated files from tracking.')
        sys.exit(1)
    
    print('✅ No forbidden files in logs/ or reports/ directories')

if __name__ == '__main__':
    main()
"
        language: system
        files: ^(logs/|reports/|tests/test_outputs/).*$
        pass_filenames: true
        always_run: false
```

## Релизный процесс

### Версионирование

Проект использует семантическое версионирование (SemVer):

- **MAJOR** (1.0.0 → 2.0.0): Несовместимые изменения API
- **MINOR** (1.0.0 → 1.1.0): Новая функциональность, обратно совместимая
- **PATCH** (1.0.0 → 1.0.1): Исправления ошибок

### Создание релиза

```bash
# 1. Обновление версии в pyproject.toml
# 2. Обновление CHANGELOG.md
# 3. Создание тега
git tag -a v1.1.0 -m "Release version 1.1.0"
git push origin v1.1.0

# 4. Создание GitHub Release
gh release create v1.1.0 --title "Release v1.1.0" --notes-file CHANGELOG.md
```

### Автоматический релиз

```yaml
# .github/workflows/release.yml
name: Release

on:
  push:
    tags:
      - 'v*'

jobs:
  release:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install build twine
      
      - name: Build package
        run: python -m build
      
      - name: Check package
        run: twine check dist/*
      
      - name: Publish to PyPI
        env:
          TWINE_USERNAME: __token__
          TWINE_PASSWORD: ${{ secrets.PYPI_API_TOKEN }}
        run: twine upload dist/*
```

## Мониторинг и алерты

### Codecov

```yaml
# .github/workflows/coverage.yml
name: Coverage

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]

jobs:
  coverage:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install .[dev]
      
      - name: Run tests with coverage
        run: |
          pytest tests/ --cov=library --cov-report=xml --cov-report=term-missing
      
      - name: Upload coverage to Codecov
        uses: codecov/codecov-action@v3
        with:
          file: ./coverage.xml
          flags: unittests
          name: codecov-umbrella
```

### Dependabot

```yaml
# .github/dependabot.yml
version: 2
updates:
  - package-ecosystem: "pip"
    directory: "/"
    schedule:
      interval: "weekly"
    open-pull-requests-limit: 10
    reviewers:
      - "SatoryKono"
    assignees:
      - "SatoryKono"
    commit-message:
      prefix: "deps"
      include: "scope"
```

### Security alerts

```yaml
# .github/workflows/security.yml
name: Security

on:
  schedule:
    - cron: '0 0 * * 1'  # Weekly
  push:
    branches: [ main ]

jobs:
  security:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install .[dev]
      
      - name: Run bandit
        run: bandit -r src/ -f json -o bandit-report.json
      
      - name: Run safety
        run: safety check --json --output safety-report.json
      
      - name: Upload security reports
        uses: actions/upload-artifact@v3
        with:
          name: security-reports
          path: |
            bandit-report.json
            safety-report.json
```

## Локальная разработка

### Настройка окружения

```bash
# 1. Клонирование репозитория
git clone https://github.com/SatoryKono/bioactivity_data_acquisition.git
cd bioactivity_data_acquisition

# 2. Создание виртуального окружения
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# или
.venv\Scripts\activate     # Windows

# 3. Установка зависимостей
pip install .[dev]

# 4. Установка pre-commit hooks
pre-commit install

# 5. Запуск тестов
pytest tests/
```

### Workflow разработки

```bash
# 1. Создание ветки
git checkout -b feature/new-pipeline

# 2. Разработка
# ... код ...

# 3. Проверка качества
make quality

# 4. Запуск тестов
make test

# 5. Коммит (pre-commit hooks запустятся автоматически)
git add .
git commit -m "feat: add new pipeline"

# 6. Push
git push origin feature/new-pipeline

# 7. Создание Pull Request
gh pr create --title "Add new pipeline" --body "Description of changes"
```

### Отладка CI/CD

```bash
# Локальный запуск GitHub Actions
act -j test

# Проверка pre-commit hooks
pre-commit run --all-files

# Проверка линтеров
black --check src/ tests/
ruff check src/ tests/
mypy src/

# Проверка безопасности
bandit -r src/
safety check
```

## Troubleshooting

### Частые проблемы

1. **Pre-commit hooks не работают**
   ```bash
   # Переустановка hooks
   pre-commit uninstall
   pre-commit install
   
   # Обновление hooks
   pre-commit autoupdate
   ```

2. **Тесты падают в CI, но работают локально**
   ```bash
   # Проверка версий Python
   python --version
   
   # Проверка зависимостей
   pip list
   
   # Запуск тестов в изолированном окружении
   docker run --rm -v $(pwd):/app bioactivity-etl:ci pytest tests/
   ```

3. **Проблемы с покрытием кода**
   ```bash
   # Проверка конфигурации coverage
   cat pyproject.toml | grep -A 10 "\[tool.coverage"
   
   # Запуск с подробным выводом
   pytest tests/ --cov=library --cov-report=term-missing -v
   ```

4. **Ошибки безопасности**
   ```bash
   # Проверка bandit
   bandit -r src/ -v
   
   # Проверка safety
   safety check --full-report
   ```

### Отладка

```bash
# Подробный вывод pytest
pytest tests/ -v -s

# Отладка конкретного теста
pytest tests/test_activity_pipeline.py::test_activity_extract -v -s

# Проверка конфигурации
python -c "import pytest; print(pytest.__version__)"
python -c "import black; print(black.__version__)"
python -c "import ruff; print(ruff.__version__)"
```
