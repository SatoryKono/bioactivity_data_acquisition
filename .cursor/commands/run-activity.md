# /run-activity
**Goal:** Запустить сбор activity‑данных напрямую.

**Defaults:**
- `INPUT`: `data\input\activity.csv`
- `CONFIG`: `configs\config_activity.yaml`
- `LIMIT`: `10`

**Steps (Agent):**
1. Активируй виртуальное окружение.
2. Выполни:
   ```bash
   python src\scripts\get_activity_data.py --input {INPUT} --config {CONFIG} --limit {LIMIT}
   ```
3. Сводка по артефактам и базовая QC‑проверка при наличии.
