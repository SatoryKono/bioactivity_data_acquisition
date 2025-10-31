# /run-assay
**Goal:** Запустить сбор ассай‑данных напрямую.

**Defaults:**
- `INPUT`: `data\input\assay.csv`
- `CONFIG`: `configs\config_assay.yaml`
- `LIMIT`: `10`

**Steps (Agent):**
1. Активируй виртуальное окружение проекта.
2. Выполни:
   ```bash
   python src\scripts\get_assay_data.py --input {INPUT} --config {CONFIG} --limit {LIMIT}
   ```
3. Дай сводку: путь/размер вывода, `row_count` (если доступен), предупреждения/ошибки.
