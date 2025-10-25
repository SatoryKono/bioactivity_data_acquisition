# E2E: Полный пайплайн

## Подготовка

```bash
python -m venv .venv
. .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r configs/requirements.txt
```

## Запуск пайплайна

```bash
bioactivity-data-acquisition pipeline --config configs/config.yaml \
  --set http.global.timeout_sec=30 \
  --set validation.strict=true
```

## Проверка результатов

- Проверьте наличие выходных CSV и отчётов QC/корреляций (пути из конфига)
- Проверьте сортировку и порядок колонок по `determinism`
