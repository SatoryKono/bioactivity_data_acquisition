# Качество, тесты и статика

## Тесты
```bash
pytest -q --cov=src --cov-fail-under=90
```

## Типы и линтеры
```bash
mypy --strict
ruff check
black --check .
```

## pre-commit
```bash
pre-commit install
pre-commit run --all-files
```

## Тестируемые примеры из документации
- Python-блоки: `pytest-codeblocks`
- CLI-примеры: дублировать в тестах с `CliRunner`
