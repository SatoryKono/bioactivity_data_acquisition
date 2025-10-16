# Качество, тесты и статика

## Тесты

```bash
pytest --cov=library --cov=tests --cov-report=term-missing --cov-fail-under=90
```

## Типы и линтеры

```bash
_mypy_target=src
mypy --strict $_mypy_target
ruff check .
black --check .
```

## pre-commit

```bash
pre-commit install
pre-commit run --all-files
```

## Тестируемые примеры из документации

- Рекомендуется `pytest-codeblocks` для Python-блоков в Markdown
- CLI-примеры дублировать в тестах (см. `tests/test_cli.py`) с `CliRunner`
- Для типов Pandera включён плагин mypy: см. `[tool.mypy] plugins = ["pandera.mypy"]` в `pyproject.toml`
