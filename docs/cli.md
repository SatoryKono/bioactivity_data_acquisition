## CLI usage (Typer)

### Общая команда
```bash
bioactivity-data-acquisition pipeline --config configs/config.yaml \
  --set runtime.workers=8 --set http.global.timeout_sec=60
```

### Documents
```bash
bioactivity-data-acquisition get-document-data -c configs/config.yaml \
  --documents-csv data/input/documents.csv --all-sources --date-tag 20250101
```

Ключевые параметры (≤10): `--config`, `--documents-csv`, `--output-dir`, `--date-tag`, `--timeout-sec`, `--retries`, `--workers`, `--limit`, `--source/--all-sources`, `--dry-run`.

### Testitem
```bash
bioactivity-data-acquisition testitem-run --config configs/config_testitem_full.yaml \
  --input data/input/testitem.csv --output data/output/testitem
```

Ключевые параметры: `--config`, `--input`, `--output`, `--cache-dir`, `--pubchem-cache-dir`, `--timeout`, `--retries`, `--limit`, `--disable-pubchem`, `--dry-run`.

### Env overrides
```bash
BIOACTIVITY__LOGGING__LEVEL=DEBUG \
  bioactivity-data-acquisition pipeline -c configs/config.yaml
```

