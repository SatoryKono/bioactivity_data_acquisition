# План синхронизации doc→code для BioETL

**Дата:** 2025-01-29  
**Версия:** 1.0.0  
**Область:** PipelineBase + Activity + Assay  
**Платформа:** локально/CI

---

## Executive Summary

1. **Подход:** doc-driven development — документация является источником истины, код должен соответствовать докам.
2. **Охват:** синхронизация базового класса `PipelineBase` и пайплайнов Activity и Assay (Production).
3. **Ключевые проверки:**
   - Линк-чек по `.lychee.toml` (83 битые ссылки, 6 отсутствующих файлов)
   - Doc→CLI doctest для примеров из README и документации пайплайнов
   - Schema-guard: проверка соответствия полей конфигов YAML моделям Pydantic
   - Determinism check: двойной прогон `--dry-run` с сравнением логов
4. **CI-воркфлоу:** интеграция проверок качества документации в CI/CD pipeline.
5. **Риски:** устаревшие примеры CLI, скрытые зависимости, неполные профили конфигов.
6. **DoD:** зелёный линк-чек, все CLI-примеры исполняются, матрица Doc↔Code покрывает 100% обязательных контрактов, GAPS_TABLE.csv и CONTRADICTIONS.md пусты или содержат только неблокирующие пункты.

---

## Источник истины и область синхронизации

### Профильные разделы документации

**Навигация:**

- `docs/INDEX.md` — карта документации
- `README.md` — разделы "Где искать документацию", "Быстрый старт", таблица поддерживаемых пайплайнов

**ETL-контракт:**

- `docs/etl_contract/01-pipeline-contract.md` — контракт `PipelineBase` (стадии `extract`, `transform`, `validate`, `write`, метод `run()`)
- `docs/etl_contract/02-pipeline-config.md` — структура конфигурации пайплайнов
- `docs/etl_contract/07-cli-integration.md` — интеграция CLI

**Каталог пайплайнов:**

- `docs/pipelines/10-pipelines-catalog.md` — каталог всех пайплайнов
- `docs/pipelines/activity-chembl/` — документация Activity (16-activity-chembl-cli.md, 17-activity-chembl-config.md)
- `docs/pipelines/assay-chembl/` — документация Assay (16-assay-chembl-cli.md, 17-assay-chembl-config.md)

**Конфиги:**

- `docs/configs/00-typed-configs-and-profiles.md` — система конфигураций Pydantic, профили `base.yaml`, `determinism.yaml`

**CLI:**

- `docs/cli/00-cli-overview.md` — обзор CLI
- `docs/cli/01-cli-commands.md` — справочник команд
- `docs/cli/02-cli-exit_codes.md` — коды выхода

### Жёсткие контракты

**API PipelineBase:**

- Стадии: `extract() -> object`, `transform(payload: object) -> object`, `validate(payload: object) -> pd.DataFrame`, `write(df: pd.DataFrame, output_path: Path, extended: bool = False) -> RunResult`
- Метод `run(output_path: Path, extended: bool = False, *args, **kwargs) -> RunResult`
- Структуры: `RunResult` (write_result, run_directory, manifest, additional_datasets, qc_summary, debug_dataset), `WriteResult` (dataset, quality_report, metadata)

**Стадии:**

- Последовательность: `extract → transform → validate → write`
- Логирование через `UnifiedLogger` с контекстом стадии
- Обработка ошибок и cleanup зарегистрированных клиентов

**Схемы/поля конфигов:**

- `PipelineConfig` (Pydantic) — обязательные: `version: Literal[1]`, `pipeline: PipelineMetadata`, `http: HTTPConfig`
- Профили: `configs/profiles/base.yaml`, `configs/profiles/determinism.yaml` — автоматически мерджатся через `include_default_profiles=True`
- Пайплайн-конфиги: `configs/pipelines/chembl/activity.yaml`, `configs/pipelines/chembl/assay.yaml` (пути согласно README)

**Флаги CLI:**

- Обязательные: `--config`, `--output-dir`
- Опциональные: `--dry-run`, `--verbose`, `--limit`, `--sample`, `--extended`, `--fail-on-schema-drift`, `--validate-columns`, `--set KEY=VALUE`
- Команды: `list`, `activity`, `assay`

**Поведение `--dry-run`:**

- Загрузка и валидация конфигурации (включая профили)
- Проверка доступности источников (handshake)
- Пропуск стадий `extract` (если нет данных) и `write`
- Выход с кодом 0 при успешной валидации

---

## Матрица трассировки Doc↔Code

| Док-пункт | Файл/символ кода | Пайплайн | Тип контракта | Статус | Действие |
|-----------|------------------|----------|---------------|--------|----------|
| `PipelineBase.run(output_path: Path, extended: bool, ...) -> RunResult` | `src/bioetl/pipelines/base.py:930` | base | обяз. | ok | — |
| `PipelineBase.write(df: pd.DataFrame, output_path: Path, extended: bool) -> RunResult` | `src/bioetl/pipelines/base.py:762` | base | обяз. | ok | — |
| `RunResult.write_result, run_directory, manifest, additional_datasets, qc_summary, debug_dataset` | `src/bioetl/pipelines/base.py:88-110` | base | обяз. | ok | — |
| `WriteResult.dataset, quality_report, metadata` | `src/bioetl/pipelines/base.py:68-85` | base | обяз. | ok | — |
| `activity_chembl` CLI команда: `python -m bioetl.cli.main activity_chembl --config configs/pipelines/chembl/activity.yaml --output-dir data/output/activity --dry-run` | `src/bioetl/cli/registry.py:26-35`, `src/bioetl/cli/app.py:40-48` | activity | обяз. | contradiction | Команда должна быть `activity_chembl`, не `activity` (CONTR-007) |
| `assay_chembl` CLI команда: `python -m bioetl.cli.main assay_chembl --config configs/pipelines/chembl/assay.yaml --output-dir data/output/assay --dry-run` | `src/bioetl/cli/registry.py:38-48` | assay | обяз. | gap | Команда не зарегистрирована (строка 114 закомментирована), требуется реализация с именем `assay_chembl` (не `assay`) |
| Профили `base.yaml` и `determinism.yaml` автоматически мерджатся | `src/bioetl/config/loader.py:14-17, 76-77`, `src/bioetl/cli/command.py:194` | base | обяз. | ok | — |
| Путь конфига Activity: `src/bioetl/configs/pipelines/chembl/activity.yaml` (документация) vs `configs/pipelines/chembl/activity.yaml` (README) | `README.md:25`, `docs/pipelines/activity-chembl/17-activity-chembl-config.md:13` | activity | обяз. | contradiction | Унифицировать путь: в README указан `configs/...`, в коде — `configs/...`, в доках — `src/bioetl/configs/...` |
| Путь конфига Assay: `configs/pipelines/chembl/assay.yaml` (README) vs отсутствует в коде | `README.md:59`, `src/bioetl/cli/registry.py:46` | assay | обяз. | gap | Создать конфиг `configs/pipelines/chembl/assay.yaml` |
| CLI флаг `--dry-run` пропускает стадии extract/write | `src/bioetl/cli/command.py:230-232`, `src/bioetl/pipelines/base.py:972` | base | обяз. | ok | — |
| `list` команда показывает зарегистрированные пайплайны | `src/bioetl/cli/app.py:26-38` | base | обяз. | ok | — |

### Обновление артефактов качества

**GAPS_TABLE.csv:**

- Добавить строку: `assay, docs/pipelines/assay-chembl/, No, No, No, No, No, No, No, HIGH` (если не реализован)

**CONTRADICTIONS.md:**

- CONTR-005: Путь конфига Activity — расхождение между README (`configs/...`) и документацией (`src/bioetl/configs/...`). Решение: унифицировать на `configs/pipelines/chembl/activity.yaml` (относительно корня проекта).

**CHECKLIST.md:**

- Отметить завершённые пункты: инвентаризация документации, каталогизация кода, матрица Doc↔Code.

---

## WBS: пошаговый план работ

### 1. Инвентаризация документации (2h)

**Задачи:**

- [x] Запустить `audit_docs.py` для сбора списка файлов и BLAKE2-хэшей
- [x] Прогнать линк-чек по `.lychee.toml`, сформировать `LINKCHECK.md`
- [x] Зафиксировать статус в `CHECKLIST.md`

**Результаты:**

- `audit_results/LINKCHECK.md` — 83 битые ссылки, 6 отсутствующих файлов
- `audit_results/GAPS_TABLE.csv` — таблица пробелов в документации

### 2. Каталогизация кода и конфигов (3h)

**Задачи:**

- [x] Извлечь сигнатуру `PipelineBase` из `src/bioetl/pipelines/base.py` (публичные стадии, хуки, методы `run()`, `write()`)
- [x] Собрать Typer-команды из `src/bioetl/cli/main.py`, `src/bioetl/cli/app.py`, `src/bioetl/cli/registry.py` (команды `list`, `activity_chembl`, `assay_chembl`, флаги `--dry-run`, `--config`, `--output-dir`)
- [x] Проанализировать Pydantic-модели в `src/bioetl/config/models.py` и профили `configs/profiles/base.yaml`, `configs/profiles/determinism.yaml`
- [x] Извлечь пути конфигов для activity/assay из README и `src/bioetl/cli/registry.py`

**Результаты:**

- Каталог символов кода: PipelineBase API, CLI-команды, модели конфигов, пути конфигов

### 3. Матрица трассировки Doc↔Code (4h)

**Задачи:**

- [ ] Сформировать таблицу «док-пункт → код → пайплайн → тип контракта → статус → действие»
- [ ] Зафиксировать пробелы в `GAPS_TABLE.csv`
- [ ] Зафиксировать противоречия в `CONTRADICTIONS.md`
- [ ] Отметить чек-пункты в `CHECKLIST.md`

**Результаты:**

- Матрица Doc↔Code (см. раздел выше)
- Обновлённые `GAPS_TABLE.csv`, `CONTRADICTIONS.md`, `CHECKLIST.md`

### 4. Семантический diff и правки (6h)

**Задачи:**

- [ ] Проверить соответствие стадий `extract/transform/validate/write`, CLI-флагов, структур конфигов документации
- [ ] Подготовить изменения кода для `PipelineBase`, `activity`, `assay` в пользу документации
- [ ] Обновить Pandera-схемы, профили, логирование при необходимости
- [ ] Обновить документацию только при наличии одобренного тикета; иначе описать расхождения в артефактах качества

**Результаты:**

- Изменения кода: унификация путей конфигов, регистрация команды `assay` (если реализована)
- Обновлённые артефакты качества

### 5. Тесты и автоматизация (5h)

**Задачи:**

- [ ] Сформировать doc-smoke скрипты: `python -m bioetl.cli.main list`, `activity_chembl` и `assay_chembl` с `--dry-run` (пути из README)
- [ ] Настроить в CI стадии: link-check, `audit_docs.py`, doc→CLI doctest, schema-guard (сверка YAML↔Pydantic), determinism check (двойной прогон `--dry-run`, сравнение логов)
- [ ] Отразить пайплайн качества и пороги фейлов в README/доках согласно профилю «CI»

**Результаты:**

- Скрипты doc-smoke тестов
- CI-конфигурация (GitHub Actions / GitLab CI)

### 6. Подготовка поставки (2h)

**Задачи:**

- [ ] Сформировать комплект изменений: код, обновлённые доки, `GAPS_TABLE.csv`, `CONTRADICTIONS.md`, `LINKCHECK.md`, закрытый `CHECKLIST.md`
- [ ] Описать риски и план отката
- [ ] Убедиться, что DoD выполняется

**Результаты:**

- PR с изменениями
- Артефакты поставки

**Итого:** ~22 часа

---

## Автоматизация и CI (pipeline качества)

### 1. Link-check (`.lychee.toml`)

**Проверка:**

```bash
lychee --config .lychee.toml
```

**Порог фейла:** критические битые ссылки (CRITICAL) блокируют PR, некритические (MEDIUM) — предупреждение.

**Отчёт:** `audit_results/LINKCHECK.md`

**CI-интеграция:**

```yaml
- name: Link Check
  run: |
    lychee --config .lychee.toml --output audit_results/LINKCHECK.md
    if [ $? -ne 0 ]; then
      echo "::error::Critical broken links detected"
      exit 1
    fi
```

### 2. Doc-audit (`audit_docs.py`)

**Проверка:**

```bash
python audit_docs.py
```

**Ожидаемые отчёты:**

- `audit_results/GAPS_TABLE.csv` — таблица пробелов
- `audit_results/LINKCHECK.md` — отчёт линк-чека
- `audit_results/CONTRADICTIONS.md` — противоречия (обновляется вручную)

**CI-интеграция:**

```yaml
- name: Doc Audit
  run: |
    python audit_docs.py
    # Проверка, что GAPS_TABLE.csv не содержит HIGH priority пробелов для base/activity/assay
```

### 3. Doc→CLI doctest

**Проверка:** автоматический запуск всех CLI-примеров из README и документации с `--dry-run`.

**Примеры из README:**

```bash
# Навигация по зарегистрированным пайплайнам
python -m bioetl.cli.main list

# Smoke-тест Activity (без побочных эффектов)
python -m bioetl.cli.main activity_chembl \
  --config configs/pipelines/chembl/activity.yaml \
  --output-dir data/output/activity \
  --dry-run

# Smoke-тест Assay (без побочных эффектов)
python -m bioetl.cli.main assay_chembl \
  --config configs/pipelines/chembl/assay.yaml \
  --output-dir data/output/assay \
  --dry-run
```

**CI-интеграция:**

```yaml
- name: CLI Doctest
  run: |
    python -m bioetl.cli.main list
    python -m bioetl.cli.main activity_chembl --config configs/pipelines/chembl/activity.yaml --output-dir data/output/activity --dry-run
    python -m bioetl.cli.main assay_chembl --config configs/pipelines/chembl/assay.yaml --output-dir data/output/assay --dry-run
```

### 4. Schema-guard

**Проверка:** соответствие описанных в доках полей конфигов реализованным моделям Pydantic.

**Пайплайн-конфиги:**

- `configs/pipelines/chembl/activity.yaml` — должен соответствовать `PipelineConfig`
- `configs/pipelines/chembl/assay.yaml` — должен соответствовать `PipelineConfig`

**CI-интеграция:**

```yaml
- name: Schema Guard
  run: |
    python -c "
    from bioetl.config import load_config
    from pathlib import Path
    
    # Проверка activity.yaml
    config = load_config('configs/pipelines/chembl/activity.yaml', include_default_profiles=True)
    assert config.pipeline.name == 'activity_chembl'
    
    # Проверка assay.yaml (если существует)
    if Path('configs/pipelines/chembl/assay.yaml').exists():
        config = load_config('configs/pipelines/chembl/assay.yaml', include_default_profiles=True)
        assert config.pipeline.name == 'assay_chembl'
    "
```

### 5. Determinism check

**Проверка:** двойной прогон activity и assay в `--dry-run` и сравнение логов/выходов.

**CI-интеграция:**

```yaml
- name: Determinism Check
  run: |
    # Первый прогон
    python -m bioetl.cli.main activity_chembl --config configs/pipelines/chembl/activity.yaml --output-dir data/output/activity_1 --dry-run > log1.txt 2>&1
    
    # Второй прогон
    python -m bioetl.cli.main activity_chembl --config configs/pipelines/chembl/activity.yaml --output-dir data/output/activity_2 --dry-run > log2.txt 2>&1
    
    # Аналогично для assay_chembl
    python -m bioetl.cli.main assay_chembl --config configs/pipelines/chembl/assay.yaml --output-dir data/output/assay_1 --dry-run > log1.txt 2>&1
    python -m bioetl.cli.main assay_chembl --config configs/pipelines/chembl/assay.yaml --output-dir data/output/assay_2 --dry-run > log2.txt 2>&1
    
    # Сравнение (игнорируя временные метки)
    diff <(sed 's/[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}T[0-9]\{2\}:[0-9]\{2\}:[0-9]\{2\}/TIMESTAMP/g' log1.txt) \
         <(sed 's/[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}T[0-9]\{2\}:[0-9]\{2\}:[0-9]\{2\}/TIMESTAMP/g' log2.txt)
```

---

## Правила разрешения конфликтов Doc↔Code

1. **Любое расхождение документируется в `CONTRADICTIONS.md`** с полями: ID, док-пункт, код, тип, приоритет, статус, решение.
2. **Добавление в матрицу:** каждый конфликт добавляется в матрицу трассировки Doc↔Code.
3. **PR/Issue:** создаётся тикет на разрешение конфликта.
4. **Приоритет:** до апрува правки доков — истина в доках; код должен быть изменён под документацию.
5. **Автор PR обязан:**
   - Синхронизировать код и доки
   - Проставить пункты в `CHECKLIST.md` и `GAPS_TABLE.csv`
   - Обновить `CONTRADICTIONS.md` при разрешении конфликта

---

## Обновления документации

### Конвенции именования

Следовать конвенциям из README:

- Файлы документации пайплайнов: `<NN>-<entity>-<source>-<topic>.md` (например, `16-activity-chembl-cli.md`)
- Навигация через `docs/INDEX.md`
- Каталог пайплайнов в `docs/pipelines/10-pipelines-catalog.md`

### Новые/обновлённые разделы

Для новых разделов придерживаться схемы `<NN>-<entity>-<source>-<topic>.md` из README.

---

## Артефакты поставки

По результату синхронизации:

1. **PR с изменениями:**
   - Код: `src/bioetl/pipelines/base.py`, `src/bioetl/cli/registry.py`, `src/bioetl/cli/command.py`
   - Конфиги: `configs/pipelines/chembl/activity.yaml`, `configs/pipelines/chembl/assay.yaml` (если создан)
   - Обновлённые доки (при необходимости)

2. **Артефакты качества:**
   - `audit_results/GAPS_TABLE.csv` — обновлённая таблица пробелов
   - `audit_results/CONTRADICTIONS.md` — список противоречий с решениями
   - `audit_results/LINKCHECK.md` — отчёт линк-чека
   - `audit_results/CHECKLIST.md` — закрытый чеклист

3. **Отчёты:**
   - Линк-чек: `audit_results/LINKCHECK.md`
   - CLI-smoke: результаты выполнения doctest
   - Schema-guard: результаты проверки конфигов

---

## Риски и митигирование

| Риск | Порог фейла | Митигирование | Шаги отката |
|------|-------------|---------------|-------------|
| Устаревшие примеры CLI в доках | HIGH | Автоматический doctest всех примеров из README/доков | Revert PR, флаг `SKIP_DOCTEST` в CI |
| Скрытые зависимости CLI (недокументированные флаги) | MEDIUM | Schema-guard проверяет соответствие CLI-флагов документации | Обновить документацию, добавить в матрицу |
| Неполные профили конфигов | MEDIUM | Проверка загрузки профилей через `load_config(..., include_default_profiles=True)` | Временный skip с тикетом |
| Flaky-линки (временные сбои сети) | LOW | Retry в линк-чеке, исключение внешних ссылок | Игнорировать некритические ссылки |

---

## Критерии приёмки (DoD)

1. **Линк-чек зелёный:** `LINKCHECK.md` без критичных ошибок (CRITICAL), некритические (MEDIUM) — предупреждения.
2. **Все CLI-примеры из доков/README исполняются с `--dry-run` без ошибок:**
   - `python -m bioetl.cli.main list`
   - `python -m bioetl.cli.main activity_chembl --config configs/pipelines/chembl/activity.yaml --output-dir data/output/activity --dry-run`
   - `python -m bioetl.cli.main assay_chembl --config configs/pipelines/chembl/assay.yaml --output-dir data/output/assay --dry-run`
3. **Матрица Doc↔Code покрывает 100% обязательных контрактов для base/activity/assay.**
4. **`GAPS_TABLE.csv` и `CONTRADICTIONS.md` пусты или содержат только неблокирующие пункты с назначенными задачами.**
5. **`CHECKLIST.md` закрыт:** все пункты отмечены как выполненные.

---

## Ограничения и стиль выполнения

1. **Любой спор трактуется в пользу документации** (до апдейта доков).
2. **Не вводить новые фреймворки** — опираться на существующие паттерны BioETL (PipelineBase, Pandera, Typer CLI, конфиги и профили, описанные в README/доках).
3. **Быть конкретным:** пути файлов, имена модулей, команды, условия фейла, форматы отчётов.

---

## Мини-примеры команд (для doctest/CI)

```bash
# Навигация по зарегистрированным пайплайнам
python -m bioetl.cli.main list

# Smoke-тест Activity (без побочных эффектов)
python -m bioetl.cli.main activity_chembl \
  --config configs/pipelines/chembl/activity.yaml \
  --output-dir data/output/activity \
  --dry-run

# Smoke-тест Assay (без побочных эффектов)
python -m bioetl.cli.main assay_chembl \
  --config configs/pipelines/chembl/assay.yaml \
  --output-dir data/output/assay \
  --dry-run
```

Пути и команды в примерах подтверждены README: CLI/конфиги для Activity и Assay.
