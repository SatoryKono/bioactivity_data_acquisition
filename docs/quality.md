# Качество, тесты и статика

## Тесты

```bash
pytest -q --cov=src --cov-fail-under=90
```

## Типы и линтеры

```bash
mypy --strict src
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
