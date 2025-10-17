# Development

## Тесты

```bash
pytest --cov=library --cov=tests --cov-report=term-missing --cov-fail-under=90
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

## CI

См. `.github/workflows/ci.yaml` — шаги: install, env override smoke, ruff, black, mypy, pytest.
