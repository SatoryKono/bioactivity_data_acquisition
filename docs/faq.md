# FAQ

## Почему получаю 429?

Проверьте лимиты источника, увеличьте backoff или уменьшите скорость.

Рецепты:

- Отключить конкретный источник в YAML: `sources.semantic_scholar.enabled: false`
- Ограничить частоту (YAML):

  ```yaml
  sources:
    chembl:
      http:
        rate_limit:
          max_calls: 1
          period: 5.0
  ```

- Переопределить через CLI: `--set sources.chembl.pagination.max_pages=1`
- Таймаут/ретраи глобально:

  ```bash
  bioactivity-data-acquisition pipeline --config configs/config.yaml \
    --set http.global.timeout_sec=60 \
    --set http.global.retries.total=10
  ```

## Как включить детерминизм экспорта?

Убедитесь, что сортировка и порядок колонок заданы в конфигурации.

Мини‑тест:

```bash
bioactivity-data-acquisition pipeline --config configs/config.yaml
cp data/output/bioactivities.csv run1.csv
bioactivity-data-acquisition pipeline --config configs/config.yaml
cp data/output/bioactivities.csv run2.csv
python - <<'PY'
from pathlib import Path
assert Path('run1.csv').read_bytes() == Path('run2.csv').read_bytes()
print('OK')
PY
```

## Как отключить Semantic Scholar?

В `configs/config.yaml`:

```yaml
sources:
  semantic_scholar:
    enabled: false
```
