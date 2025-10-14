# Project Publications ETL

Этот проект реализует ETL-пайплайн для обогащения публикаций данными из ChEMBL, Crossref,
OpenAlex, PubMed и Semantic Scholar. Пайплайн читает входной CSV-файл, валидирует его с
помощью Pandera, последовательно обращается к API внешних источников, объединяет результаты
и детерминированно записывает обогащённый CSV.

## Быстрый старт

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
fetch-publications data/input.csv data/output.csv
```

