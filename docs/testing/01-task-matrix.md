# 01 Матрица задач по тестированию release-логики ChEMBL

Документ дополняет [общую стратегию тестирования](00-testing-strategy.md) и фиксирует
конкретные задачи вместе с командами запуска. Каждая задача — это воспроизводимый
скрипт, который можно положить в чек-лист ревью или регрессионный план. Столбец
«Ссылка» указывает на файл или раздел документации с деталями реализации.

## Список задач

| Задача | Назначение | Команда | Ссылка |
| --- | --- | --- | --- |
| Юнит-тесты резолвера release | Проверяют happy-path и обработку ошибок `resolve_chembl_release`/`fetch_chembl_release` | `pytest tests/bioetl/pipelines/chembl/common/test_fetch_chembl_release.py` | [tests/bioetl/pipelines/chembl/common/test_fetch_chembl_release.py](../../tests/bioetl/pipelines/chembl/common/test_fetch_chembl_release.py) |
| Смоук всех ChEMBL-пайплайнов | Убеждаемся, что пайплайны запускаются и логируют release в dry-run | `pytest tests/bioetl/pipelines/chembl -k "release"` | [docs/testing/00-testing-strategy.md#release-fetching-tests](00-testing-strategy.md#release-fetching-tests) |
| Линтинг + типизация | Быстрая регрессия стиля и mypy перед публикацией PR | `make qa` | [Makefile](../../Makefile) |
| Проверка CLI на выборочном пайплайне | Проводим end-to-end dry-run для конкретного пайплайна, проверяя, что release попал в отчёты | `bioetl activity_chembl --config configs/pipelines/activity/activity_chembl.yaml --output-dir ./data/output --dry-run` | [docs/cli/01-cli-commands.md](../cli/01-cli-commands.md) |

## Как пользоваться матрицей

1. Выберите актуальные задачи и добавьте их в план ревью или CI. Например,
   «Пройти юнит-тесты резолвера release».
2. Запустите команду из столбца «Команда». Если вы используете `poetry` или
   `pipx`, добавьте префикс `poetry run`/`pipx run` в зависимости от окружения.
3. Сравните фактический вывод с ожидаемым поведением, описанным в связанных
   файлах. Для CLI-задачи сверяйтесь с `meta.yaml` и логами из каталога
   `data/output/<pipeline>/run_<timestamp>/`.
4. Зафиксируйте результат в чек-листе: ✅ если команда прошла, ❌ при ошибке
   (ссылка на лог обязательна).

## Дополнительные заметки

- Команды можно комбинировать в CI (например, `pytest ... && make qa`).
- Для быстрой отладки `resolve_chembl_release` допускается запуск отдельного
  тестового кейса: `pytest tests/.../test_fetch_chembl_release.py -k resolve`.
- Если пайплайн требует сетевого доступа, используйте переменную окружения
  `CHEMBL_API_BASE_URL` из `.env`, чтобы переключиться на sandbox.
