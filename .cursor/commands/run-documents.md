# /run-documents
**Goal:** Загрузить метаданные публикаций напрямую.

**Defaults:**
- `INPUT`: `data\input\documents.csv`
- `CONFIG`: `configs\config_document.yaml`
- `LIMIT`: `10`

**Steps (Agent):**
1. Активируй окружение.
2. Выполни (если скрипт присутствует):
   ```bash
   python src\scripts\get_document_data.py --input {INPUT} --config {CONFIG} --limit {LIMIT}
   ```
3. Если файла нет — сообщи явно и предложи /run-generic-script.
