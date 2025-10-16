# API limits

Скрипты мониторинга и проверки лимитов/доступности API.

## Быстрые проверки

```bash
python -m library.tools.quick_api_check
python -m library.tools.api_health_check --save
```

## Проверка лимитов

```bash
python -m library.tools.check_api_limits
python -m library.tools.check_specific_limits --source chembl
```

Параметр `--source` принимает: `chembl`, `crossref`, `openalex`, `pubmed`, `semantic_scholar`.

## Регулярный мониторинг

```bash
python -m library.tools.monitor_api
python -m library.tools.monitor_pubmed
python -m library.tools.monitor_semantic_scholar
```

См. также отчёты в каталоге `reports/`.
