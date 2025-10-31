# /run-generic-script
**Goal:** Запустить любой Python‑скрипт в проекте с аргументами.

**Inputs:**
- `SCRIPT`: относительный путь до .py (например, `src\scripts\get_assay_data.py`)
- `ARGS`: параметры командной строки (например, `--input data\input\assay.csv --config configs\config_assay.yaml --limit 10`)

**Steps (Agent):**
1. Убедись, что venv активен.
2. Выполни:
   ```bash
   python {SCRIPT} {ARGS}
   ```
3. Сообщи о статусе, выведи ключевые артефакты и их размеры.
