# CI/CD

Основной конвейер: `.github/workflows/ci.yaml`

## Триггеры

- `push` в ветки `main`, `work`
- `pull_request`

## Проверки качества

Выполняются шаги:

1. checkout
2. setup-python (3.11)
3. install deps: `pip install .[dev]`
4. smoke тест ENV override:

   ```python
   from bioactivity.config import Config
   config = Config.load("configs/config.yaml")
   assert config.runtime.log_level == "DEBUG"
   ```

5. ruff: `ruff check .`
6. black: `black --check .`
7. mypy: `mypy src`
8. pytest: `pytest`

Порог покрытия: задаётся в `configs/pyproject.toml` → `[tool.pytest.ini_options] addopts` с `--cov-fail-under=90`.
