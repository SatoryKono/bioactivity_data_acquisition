## Persistence & Determinism

### Форматы и файлы
- CSV основная выгрузка; YAML meta с `file_checksums` (SHA256)
- Пути: `data/output/<entity>/<name>_{date_tag}.csv|_qc.csv|_meta.yaml`

### Порядок колонок и NA-политика
- Порядок/сортировка управляются `determinism.*` в YAML (`column_order`, `sort.*`).
- Для target: `library.schemas.targets.TARGETS_COLUMN_ORDER`.

### Манифесты
- Верхнеуровневые: `MANIFEST.json`, `POSTPROCESS_MANIFEST.json` (ориентир структуры артефактов).

### Функции записи
- `library.etl.load.write_deterministic_csv` — единый способ детерминированной сериализации.

