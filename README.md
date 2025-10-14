# bioactivity_data_acquisition

Пайплайн извлечения метаданных публикаций для документов ChEMBL. Репозиторий содержит:

- библиотечные модули с утилитами нормализации и клиентов HTTP;
- схемы Pandera для валидации входных и выходных таблиц;
- CLI `scripts/fetch_publications.py` на Typer;
- тесты Pytest, покрывающие функции нормализации и полный ETL-поток.

## Установка окружения

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
```

## Запуск пайплайна

```bash
python scripts/fetch_publications.py run INPUT.csv OUTPUT.csv \
  --run-id local-run \
  --log-dir logs
```

Входной CSV должен содержать колонку `document_chembl_id` и, опционально, `doi`, `pmid`.
Результат — отсортированный CSV с колонками `document_chembl_id`, `doi_key`, `pmid`,
`chembl_title`, `chembl_doi`, `crossref_title`, `pubmed_title`.

Логи выводятся в stdout в формате JSON и дублируются в директорию `logs/`. Ошибки
по источникам попадают в файлы вида `<source>.error`.

## Тестирование

```bash
pytest
```

Тесты проверяют нормализацию полей и работу CLI через `typer.testing.CliRunner`.
