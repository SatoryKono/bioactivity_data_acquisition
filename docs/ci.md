# CI/CD

- Основной конвейер: `.github/workflows/ci.yaml`
- Документация: `.github/workflows/docs.yml`

## Документация (Pages)
- Сборка при push в `main`
- Публикация в ветку `gh-pages`

## Проверки качества
- pytest (порог покрытия ≥ 90%)
- mypy --strict
- ruff, black
