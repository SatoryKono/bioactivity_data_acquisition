# /run-assay-script
**Goal:** Запустить сбор данных по ассаям напрямую через Python‑скрипт.

**Defaults (edit if needed):**
- `INPUT`: `data\input\assay.csv`
- `CONFIG`: `configs\config_assay.yaml`
- `LIMIT`: `100`

**Steps (Agent):**
1. Run (Windows cmd/PowerShell compatible):
   ```bash
   python src\scripts\get_assay_data.py --input {INPUT} --config {CONFIG} --limit {LIMIT}
   ```
2.Post-checks (Agent):

2.1 Убедись, что появились/обновились целевые артефакты (CSV/Parquet) и, если скрипт создаёт, meta.yaml.

2.2 Зафиксируй краткую сводку: пути к артефактам, размеры файлов, row_count из meta.yaml при наличии.

2.3 Проверь соответствие колонок основного вывода конфигу {CONFIG}:
    - Считай column_order из YAML.
    - Сравни с фактическим заголовком выгрузки.
    - Если есть расхождения (недостающие, лишние, порядок), выведи детальный дифф и пометь запуск как failed.
    - Если настроена Pandera-валидация или QC-сайдкары, выполни их и приложи минимальный отчёт (ошибки, предупреждения, ключевые метрики).