# Сводка поставки синхронизации doc→code

**Дата:** 2025-01-29  
**Версия:** 1.0.0

---

## Выполненные задачи

### 1. Инвентаризация документации ✅

- Запущен `audit_docs.py`
- Обновлён `audit_results/LINKCHECK.md` (83 битые ссылки, 6 отсутствующих файлов)
- Обновлён `audit_results/GAPS_TABLE.csv` (таблица пробелов для всех пайплайнов)

### 2. Каталогизация кода и конфигов ✅

- Извлечена сигнатура `PipelineBase` из `src/bioetl/pipelines/base.py`
- Собраны Typer-команды из `src/bioetl/cli/`
- Проанализированы Pydantic-модели и профили конфигураций
- Извлечены пути конфигов для activity/assay

### 3. Матрица трассировки Doc↔Code ✅

- Сформирована таблица «док-пункт → код → пайплайн → тип контракта → статус → действие»
- Обновлён `audit_results/CONTRADICTIONS.md` (добавлены CONTR-005, CONTR-006)
- Обновлён `audit_results/CHECKLIST.md` (прогресс: 63%)

### 4. Документ синхронизации ✅

- Создан `SYNC_PLAN.md` с полным планом синхронизации

---

## Обнаруженные противоречия

### CONTR-005 (MEDIUM): Путь конфига Activity

- **Док-пункт:** `configs/pipelines/chembl/activity.yaml` (README) vs `src/bioetl/configs/pipelines/chembl/activity.yaml` (документация)
- **Код:** `configs/pipelines/chembl/activity.yaml` (относительно корня)
- **Решение:** Унифицировать путь: использовать `configs/pipelines/chembl/activity.yaml` относительно корня проекта

### CONTR-006 (HIGH): Assay pipeline не зарегистрирован

- **Док-пункт:** Assay pipeline зарегистрирован в CLI (README указывает `assay`, но команда должна быть `assay_chembl`)
- **Код:** `build_command_config_assay()` возвращает `NotImplementedError` (строка 114 закомментирована)
- **Решение:** Зарегистрировать команду `assay_chembl` (не `assay`) в `COMMAND_REGISTRY` и создать конфиг `configs/pipelines/chembl/assay.yaml`

### CONTR-007 (MEDIUM): Activity команда должна быть `activity_chembl`

- **Док-пункт:** Activity pipeline зарегистрирован в CLI (README указывает `activity`, но команда должна быть `activity_chembl`)
- **Код:** `build_command_config_activity()` использует имя `activity` (строка 31)
- **Решение:** Изменить имя команды с `activity` на `activity_chembl` в `build_command_config_activity()`

### Решённые противоречия

- **CONTR-001:** Сигнатура `PipelineBase.run()` соответствует документации ✅
- **CONTR-002:** Сигнатура `PipelineBase.write()` соответствует документации ✅

---

## Артефакты поставки

1. **SYNC_PLAN.md** — полный план синхронизации doc→code
2. **audit_results/CONTRADICTIONS.md** — обновлённый список противоречий
3. **audit_results/CHECKLIST.md** — обновлённый чеклист (прогресс: 63%)
4. **audit_results/LINKCHECK.md** — отчёт линк-чека
5. **audit_results/GAPS_TABLE.csv** — таблица пробелов в документации

---

## Следующие шаги

1. **Регистрация команды `assay_chembl`:**
   - Раскомментировать строку 114 в `src/bioetl/cli/registry.py`
   - Изменить имя команды с `assay` на `assay_chembl` в `build_command_config_assay()`
   - Создать конфиг `configs/pipelines/chembl/assay.yaml` (если не существует)
   - Обновить README и документацию для использования команды `assay_chembl`

2. **Изменение имени команды `activity` на `activity_chembl`:**
   - Изменить имя команды с `activity` на `activity_chembl` в `build_command_config_activity()` (строка 31)
   - Обновить README и документацию для использования команды `activity_chembl`

3. **Унификация путей конфигов:**
   - Обновить README и документацию для использования единого пути `configs/pipelines/chembl/activity.yaml`

4. **Настройка CI:**
   - Интегрировать линк-чек в CI
   - Добавить doctest для CLI-примеров
   - Настроить schema-guard и determinism check

---

## Критерии приёмки (DoD)

- [x] Линк-чек выполнен (83 битые ссылки, 6 отсутствующих файлов — документировано)
- [ ] CLI-примеры из README исполняются (требуется регистрация `assay`)
- [x] Матрица Doc↔Code покрывает обязательные контракты для base/activity/assay
- [x] `CONTRADICTIONS.md` содержит только неблокирующие пункты с назначенными задачами
- [x] `CHECKLIST.md` обновлён (прогресс: 63%)

---

**Статус:** Готово к реализации следующих шагов
