# Эксплуатация и мониторинг

## Быстрые проверки

```bash
python -m library.tools.quick_api_check
python -m library.tools.api_health_check --save
```

Параметры:

- `api_health_check`: `--save` — сохранить отчёт в `reports/`

## Проверка лимитов

```bash
python -m library.tools.check_api_limits
python -m library.tools.check_specific_limits --source chembl
```

Параметры:

- `check_specific_limits`: `--source <name>` — один из: `chembl`, `crossref`, `openalex`, `pubmed`, `semantic_scholar`

## Мониторинг по расписанию

```bash
python -m library.tools.monitor_api
python -m library.tools.monitor_pubmed
python -m library.tools.monitor_semantic_scholar
```

## Рекомендуемые интервалы

- Быстрые проверки: каждые 15 минут
- Полные отчёты лимитов: раз в час

Подробнее см. `docs/API_LIMITS_CHECK.md` и отчёты в `reports/`.
