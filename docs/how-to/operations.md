# Эксплуатация и мониторинг

## Быстрые проверки

```bash
python scripts/quick_api_check.py
python scripts/api_health_check.py --save
```

Параметры:

- `api_health_check`: `--save` — сохранить отчёт в `reports/`

## Проверка лимитов

```bash
python scripts/check_api_limits.py
python scripts/check_specific_limits.py --source chembl
```

Параметры:

- `check_specific_limits`: `--source <name>` — один из: `chembl`, `crossref`, `openalex`, `pubmed`, `semantic_scholar`

## Мониторинг по расписанию

```bash
# Используйте единую команду health для мониторинга всех API
make health CONFIG=configs/config_documents_full.yaml
```

## Рекомендуемые интервалы

- Быстрые проверки: каждые 15 минут
- Полные отчёты лимитов: раз в час

Подробнее см. `docs/API_LIMITS_CHECK.md` и отчёты в `reports/`.
