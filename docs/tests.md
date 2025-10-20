## Tests Policy

### Юнит
- Pandera схемы (assay), утилиты нормализации и валидации

### Интеграция
- Пагинация/источники (mock/record), контракты колонок/форматов, идемпотентность вывода

### Пороги качества
- pytest с покрытием ≥90% (см. `pyproject.toml`), mypy strict, ruff/black

### Запуск
```bash
pytest
mypy src
ruff check .
black --check .
```

