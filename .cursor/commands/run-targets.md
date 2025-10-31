# /run-targets
**Goal:** Загрузить target‑данные напрямую.

**Defaults:**
- `INPUT`: `data\input\target.csv`
- `CONFIG`: `configs\config_target.yaml`
- `LIMIT`: `10`

**Steps (Agent):**
1. Активируй окружение.
2. Выполни (если скрипт присутствует):
   ```bash
   python src\scripts\get_target_data.py --input {INPUT} --config {CONFIG} --limit {LIMIT}
   ```
3. Сводка по результатам; если файла нет — сообщи и предложи /run-generic-script.
