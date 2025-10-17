# Руководство по контрибьюшену

## Процесс

1. Форк/ветка от `main`
2. Изменения с тестами и обновлением документации
3. Локальные проверки: pytest, mypy, ruff, black, pre-commit
4. PR с чек-листом и ссылками на задачи

## Стиль коммитов (Conventional Commits)

Примеры:

- `feat(cli): add --limit option to get-document-data`
- `fix(semanticscholar): handle 429 with Retry-After`
- `docs(ops): add monitoring schedule examples`

## Ветвление и ревью

- Ветки формата `feat/…`, `fix/…`, `docs/…`
- Минимум один апрув
- CI зелёный: тесты ≥ 90%, линтеры чистые

## Чек-лист PR

| Пункт | Статус |
|---|---|
| Тесты проходят, покрытие ≥ 90% | [ ] |
| Линтеры чисты (mypy, ruff, black) | [ ] |
| Docs обновлены (при необходимости) | [ ] |
| CHANGELOG обновлён | [ ] |

## Локальный предпросмотр документации

```bash
pip install -r configs/requirements.txt
mkdocs serve
```

## Политика артефактов и LFS

- Не коммитьте в Git сгенерированные артефакты: `logs/`, `tests/test_outputs/`, `reports/*.{csv,json}`, `site/`, кэши (`__pycache__`, `.pytest_cache`, и т.п.).
- Крупные бинарники (> 500 КБ) должны храниться в Git LFS. Уже настроены паттерны в `.gitattributes`: `*.parquet`, `*.pkl`, `*.xlsm`, `*.png`, `*.jpg`.
- Перед первым коммитом выполните `git lfs install`.

## Pre-commit хуки

- Установите: `pip install pre-commit && pre-commit install`.
- Хуки блокируют добавление крупных файлов (>500 КБ) и артефактов из `logs/` и `reports/`.

## CI артефакты

- Отчёты покрытия, test outputs и security-отчёты публикуются в артефакты GitHub Actions (см. `Actions → Artifacts` для вашего билда).