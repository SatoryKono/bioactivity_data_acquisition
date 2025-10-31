# План рефакторинга BioETL

> **Ветка:** `@test_refactoring_32`
> **Последнее обновление:** 2025-10-31
**Актуальный аудит:** [AUDIT_REPORT_FINAL_2025.md](AUDIT_REPORT_FINAL_2025.md)

## Навигация по документам

### Основные документы (обязательные к прочтению)

1. **[REFACTOR_PLAN.md](REFACTOR_PLAN.md)** — основной план рефакторинга
   - Методика инвентаризации (§0)
   - Семьи файлов и целевые объединения (§1)
   - План по источникам (§2)
   - Порядок миграции и стратегия ветвления (§5)
   - Критерии приёмки (§10)

2. **[ACCEPTANCE_CRITERIA.md](ACCEPTANCE_CRITERIA.md)** — критерии приемки
   - Архитектура и раскладка (§A)
   - Детерминизм и идемпотентность (§C)
   - Контракты данных и валидация (§D)
   - Тестовый контур (§J)

3. **[MODULE_RULES.md](MODULE_RULES.md)** — правила организации модулей
   - Раскладка и именование (§1)
   - Границы слоёв и допустимые зависимости (§2)
   - Компоненты core/ (§15)

### Специализированные документы

4. **[PIPELINES.md](PIPELINES.md)** — описание пайплайнов
   - Контракт и жизненный цикл (§1)
   - Минимальный состав модулей (§2)
   - Архитектурные компоненты core/ (§13)

5. **[IO.md](IO.md)** — вход и вывод
   - Ввод (Input Contract) (§1)
   - Вывод (Output Contract) (§2)
   - Схемы данных (UnifiedSchema) (§3)

6. **[DATA_SOURCES.md](DATA_SOURCES.md)** — источники данных
   - Матрица источников по сущностям (§1.1)
   - Стандарт «карточки источника» (§4)
   - Нормализация и валидация (§5-6)

### Дополнительные документы

7. **[FAQ.md](FAQ.md)** — часто задаваемые вопросы
   - CLI команды и использование
   - Инвентаризация
   - Golden-наборы и property-based тесты

8. **[IO_SCHEMAS_AND_DIAGRAMS.md](IO_SCHEMAS_AND_DIAGRAMS.md)** — схемы и диаграммы
   - Диаграммы I/O для каждого пайплайна
   - Input/Output схемы
   - Mapping Input→Output

9. **[PUBCHEM_MIGRATION_PLAN.md](PUBCHEM_MIGRATION_PLAN.md)** — план миграции PubChem
   - ✅ **Статус:** Миграция завершена (2025-01-29)
   - Этапы миграции
   - Детали реализации

10. **[AUDIT_REPORT_2025.md](AUDIT_REPORT_2025.md)** — предыдущий аудит (2025-01-29)
    - Детальный анализ компонентов
    - Статус проблем
    - Метрики соответствия

11. **[AUDIT_PLAN_REPORT_2025.md](AUDIT_PLAN_REPORT_2025.md)** — аудит плана рефакторинга (2025-01-29)
    - Анализ документов плана
    - Критические, важные и средние проблемы
    - Рекомендации по приоритетам

12. **[AUDIT_REPORT_FINAL_2025.md](AUDIT_REPORT_FINAL_2025.md)** — финальный аудит (2025-10-31) ⭐ **АКТУАЛЬНЫЙ**
    - Комплексный аудит всех документов
    - Статус критических проблем
    - Метрики соответствия компонентов
    - Рекомендации по исправлениям

13. **[genera_plan.md](genera_plan.md)** — общий план
    - Цели унификации
    - Базовая структура пайплайнов
    - Единый публичный API

## Приоритеты документов

| Приоритет | Документ | Назначение |
|-----------|----------|------------|
| **P0 (Критический)** | REFACTOR_PLAN.md | Основной план рефакторинга |
| **P0 (Критический)** | ACCEPTANCE_CRITERIA.md | Критерии приемки |
| **P0 (Критический)** | MODULE_RULES.md | Правила организации модулей |
| **P1 (Важный)** | PIPELINES.md | Описание пайплайнов |
| **P1 (Важный)** | IO.md | Вход и вывод |
| **P1 (Важный)** | DATA_SOURCES.md | Источники данных |
| **P2 (Средний)** | FAQ.md | Часто задаваемые вопросы |
| **P2 (Средний)** | IO_SCHEMAS_AND_DIAGRAMS.md | Схемы и диаграммы |
| **P2 (Средний)** | PUBCHEM_MIGRATION_PLAN.md | План миграции PubChem (завершено) |
| **P2 (Средний)** | AUDIT_REPORT_2025.md | Предыдущий аудит |
| **P2 (Средний)** | AUDIT_PLAN_REPORT_2025.md | Аудит плана рефакторинга |
| **P2 (Средний)** | genera_plan.md | Общий план |

## Быстрая справка

### Структура модулей
- **Раскладка:** см. [MODULE_RULES.md](MODULE_RULES.md) §1
- **Зависимости:** см. [MODULE_RULES.md](MODULE_RULES.md) §2
- **Компоненты core/:** см. [MODULE_RULES.md](MODULE_RULES.md) §15

### Критерии приёмки
- **Архитектура:** см. [ACCEPTANCE_CRITERIA.md](ACCEPTANCE_CRITERIA.md) §A
- **Детерминизм:** см. [ACCEPTANCE_CRITERIA.md](ACCEPTANCE_CRITERIA.md) §C
- **Тесты:** см. [ACCEPTANCE_CRITERIA.md](ACCEPTANCE_CRITERIA.md) §J

### Источники данных
- **Матрица источников:** см. [DATA_SOURCES.md](DATA_SOURCES.md) §1.1
- **Нормализация:** см. [DATA_SOURCES.md](DATA_SOURCES.md) §5
- **Валидация:** см. [DATA_SOURCES.md](DATA_SOURCES.md) §6

### Пайплайны
- **Контракт:** см. [PIPELINES.md](PIPELINES.md) §1
- **Состав модулей:** см. [PIPELINES.md](PIPELINES.md) §2
- **Компоненты:** см. [PIPELINES.md](PIPELINES.md) §13

### Вход и вывод
- **Input Contract:** см. [IO.md](IO.md) §1
- **Output Contract:** см. [IO.md](IO.md) §2
- **Схемы данных:** см. [IO.md](IO.md) §3

## Ключевые проблемы и статус

> **Актуальный статус:** см. [AUDIT_REPORT_FINAL_2025.md](AUDIT_REPORT_FINAL_2025.md) (2025-10-31)

### Критические проблемы (P0)

| ID | Проблема | Статус | Документ |
|----|----------|--------|----------|
| P0-1 | Неразрешенный конфликт слияния | ✅ **ИСПРАВЛЕНО** | [AUDIT_REPORT_FINAL_2025.md](AUDIT_REPORT_FINAL_2025.md#p0-1) |
| P0-2 | Отсутствие артефакта инвентаризации | ✅ **ИСПРАВЛЕНО** | [AUDIT_REPORT_FINAL_2025.md](AUDIT_REPORT_FINAL_2025.md#p0-2) |
| P0-3 | Расхождения в структуре тестов | ⚠️ **ЧАСТИЧНО** | [AUDIT_REPORT_FINAL_2025.md](AUDIT_REPORT_FINAL_2025.md#p0-3) |

### Важные проблемы (P1)

| ID | Проблема | Статус | Документ |
|----|----------|--------|----------|
| P1-1 | Отсутствие property-based тестов | ❌ **НЕ ИСПРАВЛЕНО** | [AUDIT_REPORT_FINAL_2025.md](AUDIT_REPORT_FINAL_2025.md#p1-1) |
| P1-2 | Отсутствие проверки бит-идентичности | ❌ **НЕ ИСПРАВЛЕНО** | [AUDIT_REPORT_FINAL_2025.md](AUDIT_REPORT_FINAL_2025.md#p1-2) |
| P1-3 | APIConfig vs TargetSourceConfig | ⚠️ **ТРЕБУЕТ ДОКУМЕНТИРОВАНИЯ** | [AUDIT_REPORT_FINAL_2025.md](AUDIT_REPORT_FINAL_2025.md#p1-3) |

**Полный список проблем:** см. [AUDIT_REPORT_FINAL_2025.md](AUDIT_REPORT_FINAL_2025.md)

## Зависимости между документами

```
REFACTOR_PLAN.md (основной)
├── MODULE_RULES.md (правила модулей)
├── ACCEPTANCE_CRITERIA.md (критерии приёмки)
├── PIPELINES.md (описание пайплайнов)
├── IO.md (вход и вывод)
├── DATA_SOURCES.md (источники данных)
├── FAQ.md (часто задаваемые вопросы)
├── IO_SCHEMAS_AND_DIAGRAMS.md (схемы и диаграммы)
├── PUBCHEM_MIGRATION_PLAN.md (план миграции PubChem)
├── AUDIT_REPORT_2025.md (предыдущий аудит)
├── AUDIT_PLAN_REPORT_2025.md (аудит плана)
└── genera_plan.md (общий план)
```

## Источники истины

- **[docs/requirements/PIPELINES.inventory.csv](../docs/requirements/PIPELINES.inventory.csv)** и **[docs/requirements/PIPELINES.inventory.clusters.md](../docs/requirements/PIPELINES.inventory.clusters.md)** — актуальные артефакты инвентаризации; актуальность подтверждается командой `python src/scripts/run_inventory.py --check --config configs/inventory.yaml` (локально и в CI job `inventory-check`)
- **[docs/requirements/SOURCES_AND_INTERFACES.md](../docs/requirements/SOURCES_AND_INTERFACES.md)** — витрина спецификаций источников и интерфейсов
- **[configs/inventory.yaml](../configs/inventory.yaml)** — конфигурация генератора инвентаризации
- **[src/scripts/run_inventory.py](../src/scripts/run_inventory.py)** — CLI для генерации артефактов
- **[DEPRECATIONS.md](../DEPRECATIONS.md)** — реестр депрекаций
- **[PROJECT_RULES.md](../PROJECT_RULES.md)** — правила проекта

## Контекст проекта

**Ветка:** `@test_refactoring_32`
**Репозиторий:** SatoryKono/bioactivity_data_acquisition
**Цель:** Рефакторинг ETL системы для биоактивности данных
**Основные источники:** ChEMBL, UniProt, PubChem, Crossref, PubMed, OpenAlex, Semantic Scholar, IUPHAR

## Связь с другими документами

- **[PROJECT_RULES.md](../PROJECT_RULES.md)** — правила проекта
- **[USER_RULES.md](../USER_RULES.md)** — правила пользователя
- **[README.md](../README.md)** — основной README проекта
- **[CHANGELOG.md](../CHANGELOG.md)** — история изменений

---

**Примечание:** Документы в папке `refactoring/` описывают план рефакторинга проекта. Для актуальной документации компонентов см. `docs/requirements/`.
