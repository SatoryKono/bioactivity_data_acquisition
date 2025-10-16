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

- [ ] Тесты проходят, покрытие ≥ 90%
- [ ] Линтеры чисты (mypy, ruff, black)
- [ ] Docs обновлены (при необходимости)
- [ ] CHANGELOG обновлён
