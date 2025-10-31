# /run-testitems
**Goal:** Загрузить testitem‑данные напрямую.

**Defaults:**
- `INPUT`: `data\input\testitem.csv`
- `CONFIG`: `configs\config_testitem.yaml`
- `LIMIT`: `10`

**Steps (Agent):**
1. Активируй окружение.
2. Выполни (если скрипт присутствует):
   ```bash
   python src\scripts\get_testitem_data.py --input {INPUT} --config {CONFIG} --limit {LIMIT}
   ```
3. Сводка по артефактам и QC при наличии.
