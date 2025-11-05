# План синхронизации Doc→Code для BioETL

**Репозиторий**: SatoryKono/bioactivity_data_acquisition  
**Ветка**: refactoring_002-1  
**Дата создания**: 2025-01-29  
**Целевая платформа**: CI (локально/CI)  
**Политика детерминизма**: strict  
**Область**: base + activity + assay  
**Порог фейла**: zero-gap  
**Форматы отчётов**: md, csv

---

## Executive Summary

1. **Подход doc→code**: Документация — источник истины. При расхождениях код приводится к докам; исключения фиксируются в тикетах на апдейт доков с обоснованием.

2. **Охват синхронизации**: Базовый каркас (`PipelineBase`), пайплайны Activity и Assay (Production статус согласно README), CLI-команды (`activity_chembl`, `assay_chembl`), конфигурации (`configs/pipelines/chembl/*.yaml`).

3. **Ключевые проверки**:
   - Link-check через `.lychee.toml` (отчёт в `LINKCHECK.md`)
   - Doc-test CLI примеров из README/доков с `--dry-run`
   - Schema-guard: проверка соответствия полей конфигов Pydantic-моделям
   - Determinism check: двойной прогон с сравнением выходов

4. **CI-воркфлоу**: Отдельные джобы для link-check, doc-audit (`audit_docs.py`), doctest CLI, schema-guard, determinism check. Фейл при критичных несоответствиях.

5. **Риски**: Устаревшие примеры в доках (особенно CLI команды `activity` vs `activity_chembl`), скрытые зависимости в CLI, неполные профили конфигов, flaky-линки в markdown.

6. **DoD**: Зелёный link-check, все CLI-примеры выполняются с `--dry-run`, матрица Doc↔Code покрывает 100% обязательных контрактов, `GAPS_TABLE.csv` и `CONTRADICTIONS.md` пусты или содержат только неблокирующие пункты, `CHECKLIST.md` закрыт.

7. **Артефакты поставки**: PR с изменениями в коде (base/activity/assay), обновлённые доки при необходимости, актуальные `GAPS_TABLE.csv` и `CONTRADICTIONS.md`, отчёты линк-чека и doctest/CLI-smoke.

8. **Навигация**: README (`docs/INDEX.md`) — главный навигатор по докам. Интерфейсы CLI/конфигов описаны в README (раздел «Поддерживаемые источники данных»).

---

## Источник истины и область синхронизации

### Профильные разделы документации

**Навигация**:
- `docs/INDEX.md` — карта всей документации
- `README.md` — быстрый старт, ссылки на ключевые разделы

**ETL-контракт** (жёсткие контракты):
- `docs/etl_contract/00-etl-overview.md` — обзор принципов
- `docs/etl_contract/01-pipeline-contract.md` — **API PipelineBase** (стадии, сигнатуры методов)
- `docs/etl_contract/02-pipeline-config.md` — структура конфигов
- `docs/etl_contract/07-cli-integration.md` — интеграция CLI

**Каталог пайплайнов**:
- `docs/pipelines/00-pipeline-base.md` — спецификация PipelineBase
- `docs/pipelines/10-pipelines-catalog.md` — каталог всех пайплайнов
- `docs/pipelines/activity-chembl/00-activity-chembl-overview.md` — обзор Activity
- `docs/pipelines/assay-chembl/00-assay-chembl-overview.md` — обзор Assay

**Конфиги** (жёсткие контракты):
- `docs/configs/00-typed-configs-and-profiles.md` — типизированные конфиги, Pydantic-модели
- Профили: `configs/profiles/base.yaml`, `configs/profiles/determinism.yaml`

**CLI** (жёсткие контракты):
- `docs/cli/00-cli-overview.md` — обзор CLI
- `docs/cli/01-cli-commands.md` — **команды и флаги CLI**
- `docs/cli/02-cli-exit_codes.md` — коды выхода

**Детерминизм** (жёсткие контракты):
- `docs/determinism/00-determinism-policy.md` — политика детерминизма
- Сортировки: Activity — `["assay_id", "testitem_id", "activity_id"]`, Assay — `["assay_id"]`

### Типы контрактов

**Обязательные (жёсткие)**:
- API `PipelineBase`: сигнатуры `extract()`, `transform()`, `validate()`, `write()`, `run()`
- Стадии жизненного цикла: `extract → transform → validate → write`
- Структуры `RunResult` и `WriteResult` (обязательные поля)
- Флаги CLI: `--config`, `--output-dir`, `--dry-run`, `--limit`, `--set`
- Поведение `--dry-run`: валидация конфигов без выполнения пайплайна
- Поля конфигов: структура `PipelineConfig`, профили `base.yaml`/`determinism.yaml`
- Сортировки детерминизма: ключи из конфигов пайплайнов

**Рекомендуемые**:
- Расширенные поля `RunResult`/`WriteResult` (`additional_datasets`, `qc_summary`, `debug_dataset`)
- Опциональные флаги CLI (`--sample`, `--golden`, `--mode`)
- Дополнительные поля в конфигах (enrichment, адаптеры)

---

## Матрица трассировки Doc↔Code

| Док-пункт | Файл/символ кода | Пайплайн | Тип контракта | Статус | Действие |
|-----------|------------------|----------|---------------|--------|----------|
| `docs/etl_contract/01-pipeline-contract.md`: `PipelineBase.extract() -> pd.DataFrame` | `src/bioetl/pipelines/base.py:45` (abstract) | base | обяз. | ok | - |
| `docs/etl_contract/01-pipeline-contract.md`: `PipelineBase.extract()` без параметров | `src/bioetl/pipelines/base.py:81` (abstract `*args, **kwargs`) | base | обяз. | contradiction | CONTR-001: привести к `extract(self) -> pd.DataFrame` или обновить доки |
| `docs/pipelines/00-pipeline-base.md`: `PipelineBase.run(output_path: Path, extended: bool, ...) -> RunResult` | `src/bioetl/pipelines/base.py:109` (`run(...)` сигнатура отличается) | base | обяз. | gap | CONTR-001: проверить реальную сигнатуру и синхронизировать |
| `docs/pipelines/00-pipeline-base.md`: `PipelineBase.write(df: pd.DataFrame, output_path: Path, extended: bool) -> RunResult` | `src/bioetl/pipelines/base.py:write()` (возвращает `RunResult`, но сигнатура отличается) | base | обяз. | contradiction | CONTR-002: привести сигнатуру к документации |
| `docs/pipelines/00-pipeline-base.md`: `WriteResult` поля `dataset`, `quality_report`, `metadata` | `src/bioetl/pipelines/base.py:69` (`WriteResult` имеет дополнительные поля) | base | обяз. | contradiction | CONTR-003: обновить доки или сделать поля опциональными |
| `docs/pipelines/00-pipeline-base.md`: `RunResult` поля `write_result`, `run_directory`, `manifest`, `additional_datasets`, `qc_summary`, `debug_dataset` | `src/bioetl/pipelines/base.py:89` (`RunResult` имеет `run_id`, `log_file`, `stage_durations_ms` вместо некоторых полей) | base | обяз. | contradiction | CONTR-004: синхронизировать структуру |
| `README.md`: CLI команда `activity` | `src/bioetl/cli/registry.py:31` (`activity_chembl`) | activity | обяз. | contradiction | CONTR-007: унифицировать имя команды (`activity` в README vs `activity_chembl` в коде) |
| `README.md`: CLI команда `assay` | `src/bioetl/cli/registry.py:43` (`assay_chembl`) | assay | обяз. | contradiction | CONTR-006: унифицировать имя команды |
| `README.md`: конфиг Activity `configs/pipelines/chembl/activity.yaml` | `src/bioetl/cli/registry.py:34` (`configs/pipelines/chembl/activity.yaml`) | activity | обяз. | ok | - |
| `docs/cli/01-cli-commands.md`: CLI команда `activity` | `src/bioetl/cli/registry.py:31` (`activity_chembl`) | activity | обяз. | contradiction | CONTR-007: унифицировать |
| `docs/cli/01-cli-commands.md`: CLI команда `assay` | `src/bioetl/cli/registry.py:43` (`assay_chembl`) | assay | обяз. | contradiction | CONTR-006: унифицировать |
| `docs/determinism/00-determinism-policy.md`: Activity sort keys `["assay_id", "testitem_id", "activity_id"]` | `configs/pipelines/chembl/activity.yaml:32` (`["assay_chembl_id", "testitem_chembl_id", "activity_id"]`) | activity | обяз. | contradiction | Проверить соответствие имён колонок (возможно, доки используют короткие имена) |
| `docs/determinism/00-determinism-policy.md`: Assay sort key `["assay_id"]` | `configs/pipelines/chembl/assay.yaml:32` (`["assay_chembl_id", "row_subtype", "row_index"]`) | assay | обяз. | contradiction | Проверить: доки указывают упрощённую версию, конфиг — полную |
| `docs/configs/00-typed-configs-and-profiles.md`: применение профилей `base.yaml` и `determinism.yaml` | `src/bioetl/cli/command.py` (через `load_config`) | base | обяз. | ok | Проверить реализацию `load_config` |
| `README.md`: пример CLI `python -m bioetl.cli.main activity --config ... --dry-run` | `src/bioetl/cli/app.py:list_commands()` | activity | обяз. | gap | Добавить doctest для примера из README |

**Примечания к матрице**:
- Контрадикции CONTR-001–CONTR-007 документированы в `audit_results/CONTRADICTIONS.md`
- Пробелы (gaps) требуют дополнения кода/доков
- Статус "ok" означает соответствие

---

## WBS: Пошаговый план работ

### 1. Инвентаризация документации (T-shirt: S, часы: 2)

**Задачи**:
- Собрать список всех `.md` файлов в `docs/` с хэшами (SHA256)
- Прогнать линк-чек через `lychee --config .lychee.toml` и сформировать `audit_results/LINKCHECK.md`
- Выявить отсутствующие файлы, упомянутые в `docs/INDEX.md`

**Команды**:

```bash
# Хэши документации
find docs -name "*.md" -exec sha256sum {} \; > docs_hashes.txt

# Линк-чек
lychee --config .lychee.toml --no-progress --verbose > audit_results/LINKCHECK.md 2>&1
```

**Результат**: `LINKCHECK.md` с результатами, список всех `.md` файлов с хэшами.

---

### 2. Каталогизация символов кода (T-shirt: M, часы: 4)

**Задачи**:
- Извлечь сигнатуры `PipelineBase` (методы, типы возврата)
- Извлечь CLI-команды из `COMMAND_REGISTRY` (`activity_chembl`, `assay_chembl`)
- Извлечь модели конфигов (`PipelineConfig`, профили)
- Зафиксировать пути конфигов: `configs/pipelines/chembl/activity.yaml`, `configs/pipelines/chembl/assay.yaml`

**Команды**:

```bash
# Сигнатуры PipelineBase
python -c "from src.bioetl.pipelines.base import PipelineBase; import inspect; print(inspect.signature(PipelineBase.extract))"

# CLI команды
python -m bioetl.cli.main list

# Проверка конфигов
python -c "from bioetl.config import load_config; c = load_config('configs/pipelines/chembl/activity.yaml'); print(c.pipeline.name)"
```

**Результат**: Таблица символов кода (модуль, класс, метод, сигнатура).

---

### 3. Семантический diff Doc→Code (T-shirt: L, часы: 8)

**Задачи**:
- Сопоставить API стадий: `extract()`, `transform()`, `validate()`, `write()`, `run()`
- Сопоставить поля конфигов: `pipeline.name`, `sources.chembl.batch_size`, `determinism.sort.by`
- Сопоставить параметры CLI: `--config`, `--output-dir`, `--dry-run`
- Проверить поведение `--dry-run`: должен валидировать конфиги без выполнения

**Методология**:
1. Парсинг доков: извлечение блоков кода с сигнатурами методов
2. Парсинг кода: AST-анализ `src/bioetl/pipelines/base.py` для извлечения сигнатур
3. Сопоставление: сравнение сигнатур, типов возврата, параметров
4. Классификация: `ok`, `gap`, `contradiction`

**Результат**: Обновлённый `audit_results/CONTRADICTIONS.md` и `audit_results/GAPS_TABLE.csv` (создать, если отсутствует).

---

### 4. Правки кода под документацию (T-shirt: XL, часы: 16)

**Приоритеты** (по CONTRADICTIONS.md):
1. **CONTR-006 (HIGH)**: Унифицировать имя команды Assay (`assay` → `assay_chembl` в CLI или наоборот)
2. **CONTR-007 (MEDIUM)**: Унифицировать имя команды Activity
3. **CONTR-001 (HIGH)**: Привести `PipelineBase.run()` к сигнатуре из доков
4. **CONTR-002 (HIGH)**: Привести `PipelineBase.write()` к сигнатуре из доков
5. **CONTR-003/004 (MEDIUM)**: Синхронизировать структуры `WriteResult`/`RunResult`

**Правила**:
- Код изменяется под документацию
- Если доки устарели — создаётся issue с обоснованием, но до апрува доки остаются истиной
- Все изменения проходят тесты: `pytest tests/`

**Результат**: PR с изменениями в `src/bioetl/pipelines/base.py`, `src/bioetl/cli/registry.py`, при необходимости — в пайплайнах.

---

### 5. Doc-тесты CLI (T-shirt: M, часы: 6)

**Задачи**:
- Извлечь примеры CLI из README и `docs/cli/01-cli-commands.md`
- Создать smoke-тесты для всех примеров с `--dry-run`

**Примеры из README**:

```bash
python -m bioetl.cli.main list
python -m bioetl.cli.main activity_chembl \
  --config configs/pipelines/chembl/activity.yaml \
  --output-dir data/output/activity \
  --dry-run
python -m bioetl.cli.main assay_chembl \
  --config configs/pipelines/chembl/assay.yaml \
  --output-dir data/output/assay \
  --dry-run
```

**Реализация**: Создать `tests/integration/test_cli_doctest.py`:
```python
import subprocess
import pytest

def test_cli_list():
    result = subprocess.run(["python", "-m", "bioetl.cli.main", "list"], 
                           capture_output=True, text=True)
    assert result.returncode == 0
    assert "activity_chembl" in result.stdout

def test_cli_activity_dry_run():
    result = subprocess.run([
        "python", "-m", "bioetl.cli.main", "activity_chembl",
        "--config", "configs/pipelines/chembl/activity.yaml",
        "--output-dir", "data/output/activity",
        "--dry-run"
    ], capture_output=True, text=True)
    assert result.returncode == 0

def test_cli_assay_dry_run():
    result = subprocess.run([
        "python", "-m", "bioetl.cli.main", "assay_chembl",
        "--config", "configs/pipelines/chembl/assay.yaml",
        "--output-dir", "data/output/assay",
        "--dry-run"
    ], capture_output=True, text=True)
    assert result.returncode == 0
```

**Результат**: `tests/integration/test_cli_doctest.py`, все тесты проходят.

---

### 6. Валидация профилей конфигов (T-shirt: S, часы: 3)

**Задачи**:
- Загрузить `configs/profiles/base.yaml` и `configs/profiles/determinism.yaml`
- Проверить соответствие Pydantic-моделям `PipelineConfig`
- Убедиться, что CLI автоматически применяет эти профили

**Команды**:
```bash
python -c "
from bioetl.config import load_config
from pathlib import Path

# Проверка base.yaml
base = load_config('configs/profiles/base.yaml')
print('base.yaml loaded:', base.pipeline.name if hasattr(base, 'pipeline') else 'OK')

# Проверка determinism.yaml
det = load_config('configs/profiles/determinism.yaml')
print('determinism.yaml loaded:', det.determinism.sort.by if hasattr(det, 'determinism') else 'OK')
"
```

**Результат**: Отчёт о валидности профилей, исправление ошибок валидации при необходимости.

---

### 7. Обновление артефактов качества (T-shirt: M, часы: 4)

**Задачи**:
- Обновить `audit_results/GAPS_TABLE.csv` (создать, если отсутствует) с выявленными пробелами
- Обновить `audit_results/CONTRADICTIONS.md` с новыми контрадикциями
- Отметить пункты в `audit_results/CHECKLIST.md` как выполненные

**Формат GAPS_TABLE.csv**:
```csv
gap_id,doc_reference,code_reference,pipeline,severity,action,status
GAP-001,docs/cli/01-cli-commands.md:activity,src/bioetl/cli/registry.py:activity_chembl,activity,HIGH,Unify command name,open
```

**Результат**: Актуальные `GAPS_TABLE.csv`, `CONTRADICTIONS.md`, обновлённый `CHECKLIST.md`.

---

## Автоматизация и CI (Pipeline качества)

### 1. Link-check (.lychee.toml)

**Джоба CI**: `.github/workflows/ci.yaml` → `link-check`

**Команда**:
```bash
lychee --config .lychee.toml --no-progress --verbose
```

**Критерий фейла**: Любые битые внутренние ссылки (не HTTP/HTTPS/mailto)

**Отчёт**: Сохранять в `audit_results/LINKCHECK.md`

---

### 2. Doc-audit (audit_docs.py)

**Джоба CI**: `.github/workflows/ci.yaml` → `doc-audit` (новая джоба)

**Команда**:
```bash
python audit_docs.py
```

**Проверки**:
- Наличие всех обязательных разделов документации для каждого пайплайна
- Соответствие имён файлов конвенции `<NN>-<entity>-<source>-<topic>.md`
- Наличие ссылок на все файлы в `docs/INDEX.md`

**Отчёт**: JSON/CSV с результатами проверок

**Критерий фейла**: Отсутствие обязательных разделов для Production пайплайнов (Activity, Assay)

---

### 3. Doc→CLI doctest

**Джоба CI**: `.github/workflows/ci.yaml` → `cli-doctest` (новая джоба)

**Команда**:
```bash
pytest tests/integration/test_cli_doctest.py -v
```

**Проверки**:
- Все CLI-примеры из README выполняются с `--dry-run`
- Команда `list` работает и показывает `activity_chembl`, `assay_chembl`
- Все примеры завершаются с кодом 0

**Критерий фейла**: Любой тест падает

---

### 4. Schema-guard

**Джоба CI**: `.github/workflows/ci.yaml` → `schema-guard` (новая джоба)

**Команда**:
```bash
python -c "
from bioetl.config import load_config
from pathlib import Path

# Проверка конфигов Activity и Assay
for pipeline in ['activity', 'assay']:
    config_path = f'configs/pipelines/chembl/{pipeline}.yaml'
    try:
        config = load_config(config_path)
        print(f'{pipeline}: OK')
    except Exception as e:
        print(f'{pipeline}: FAIL - {e}')
        exit(1)
"
```

**Проверки**:
- Конфиги `configs/pipelines/chembl/activity.yaml` и `configs/pipelines/chembl/assay.yaml` загружаются без ошибок
- Все поля конфигов соответствуют `PipelineConfig` Pydantic-модели
- Профили `base.yaml` и `determinism.yaml` применяются автоматически

**Критерий фейла**: Ошибка валидации Pydantic

---

### 5. Determinism check

**Джоба CI**: `.github/workflows/ci.yaml` → `determinism-check` (новая джоба)

**Команда**:
```bash
# Первый прогон
python -m bioetl.cli.main activity_chembl \
  --config configs/pipelines/chembl/activity.yaml \
  --output-dir data/output/activity_test1 \
  --dry-run

# Второй прогон (должен дать идентичный результат)
python -m bioetl.cli.main activity_chembl \
  --config configs/pipelines/chembl/activity.yaml \
  --output-dir data/output/activity_test2 \
  --dry-run

# Сравнение логов (если есть артефакты в --dry-run)
# diff -u data/output/activity_test1/*.log data/output/activity_test2/*.log
```

**Проверки**:
- Два последовательных прогона с одинаковыми параметрами дают идентичные логи (для детерминированных полей)
- В реальном прогоне (без `--dry-run`) выходные файлы имеют идентичные хэши при идентичных входах

**Критерий фейла**: Различия в детерминированных артефактах между прогонами

---

## Правила разрешения конфликтов Doc↔Code

1. **Любое расхождение документируется** в `audit_results/CONTRADICTIONS.md` с присвоением ID (CONTR-XXX).

2. **Классификация**:
   - **HIGH**: Критичные расхождения (API методы, обязательные поля, CLI команды)
   - **MEDIUM**: Расхождения в опциональных полях, дополнительных функциях
   - **LOW**: Стилистические различия, не влияющие на функциональность

3. **Процесс разрешения**:
   - Создать issue/PR с описанием расхождения
   - До апрува правки доков — код приводится к документации
   - После апрува — синхронизируются код и доки, обновляется `CONTRADICTIONS.md`

4. **Матрица обновляется** синхронно с разрешением конфликтов.

5. **CHECKLIST.md** обновляется при закрытии каждого пункта.

---

## Обновления документации

**Конвенции**:
- Именование файлов: `<NN>-<entity>-<source>-<topic>.md` (см. `docs/styleguide/00-naming-conventions.md`)
- Навигация через `docs/INDEX.md`
- Ссылки на код: `[ref: repo:path@branch]`

**Для новых/обновлённых разделов**:
- Обновить `docs/INDEX.md` при добавлении новых файлов
- Проверить все внутренние ссылки через линк-чек
- Обновить `CHANGELOG.md` при breaking changes

**Синхронизация с кодом**:
- При изменении API `PipelineBase` — обновить `docs/etl_contract/01-pipeline-contract.md` и `docs/pipelines/00-pipeline-base.md`
- При изменении CLI — обновить `docs/cli/01-cli-commands.md` и README
- При изменении конфигов — обновить `docs/configs/00-typed-configs-and-profiles.md`

---

## Артефакты поставки

По результату синхронизации:

1. **PR с изменениями**:
   - `src/bioetl/pipelines/base.py` (при необходимости)
   - `src/bioetl/cli/registry.py` (унификация имён команд)
   - `src/bioetl/pipelines/chembl/activity.py` (при необходимости)
   - `src/bioetl/pipelines/chembl/assay.py` (при необходимости)

2. **Обновлённые доки** (при необходимости):
   - `docs/cli/01-cli-commands.md` (унификация имён команд)
   - `docs/pipelines/00-pipeline-base.md` (синхронизация сигнатур)

3. **Артефакты качества**:
   - `audit_results/GAPS_TABLE.csv` (создан/обновлён)
   - `audit_results/CONTRADICTIONS.md` (обновлён, контрадикции разрешены или задокументированы)
   - `audit_results/CHECKLIST.md` (закрыт)
   - `audit_results/LINKCHECK.md` (обновлён результатами линк-чека)

4. **Отчёты**:
   - `audit_results/CLI_DOCTEST_REPORT.md` (результаты smoke-тестов CLI)
   - `audit_results/SCHEMA_GUARD_REPORT.md` (результаты валидации конфигов)

5. **Тесты**:
   - `tests/integration/test_cli_doctest.py` (новый файл с smoke-тестами)

---

## Риски и митигирование

| Риск | Вероятность | Влияние | Порог фейла | Митигирование |
|------|-------------|---------|-------------|---------------|
| Устаревшие примеры CLI в README | Высокая | Высокое | CLI-пример не выполняется | Добавить doctest для всех примеров, автоматизировать проверку в CI |
| Скрытые зависимости CLI (импорты, конфиги) | Средняя | Среднее | Пайплайн не запускается | Добавить smoke-тесты с `--dry-run` для всех команд |
| Неполные профили конфигов | Средняя | Среднее | Валидация Pydantic падает | Schema-guard джоба в CI, проверка загрузки всех профилей |
| Flaky-линки в markdown | Низкая | Низкое | Линк-чек падает из-за временных проблем | Retry в CI, исключение внешних ссылок из критичных проверок |
| Несоответствие имён CLI команд | Высокая | Высокое | Пользователи не могут запустить пайплайны | Унифицировать имена, обновить README и доки |

**Шаги отката**:
- Revert PR при критичных ошибках
- Флаг `--skip-doc-sync` в CI для временного обхода проверок
- Временный skip с тикетом: добавить `[skip-doc-sync]` в commit message

---

## Критерии приёмки (DoD)

1. ✅ **Линк-чек зелёный**: `audit_results/LINKCHECK.md` без критичных ошибок (битые внутренние ссылки отсутствуют).

2. ✅ **Все CLI-примеры из доков/README выполняются**: 
   - `python -m bioetl.cli.main list` → код 0
   - `python -m bioetl.cli.main activity_chembl --config ... --dry-run` → код 0
   - `python -m bioetl.cli.main assay_chembl --config ... --dry-run` → код 0

3. ✅ **Матрица Doc↔Code покрывает 100% обязательных контрактов**: Все методы `PipelineBase`, все CLI команды, все обязательные поля конфигов.

4. ✅ **GAPS_TABLE.csv и CONTRADICTIONS.md пусты или содержат только неблокирующие пункты**: Все HIGH-приоритетные контрадикции разрешены.

5. ✅ **CHECKLIST.md закрыт**: Все пункты отмечены как выполненные.

6. ✅ **CI пайплайн качества зелёный**: Все джобы (link-check, doc-audit, cli-doctest, schema-guard, determinism-check) проходят.

---

## Ограничения и стиль выполнения

1. **Любой спор трактуется в пользу документации** (до апрува правки доков).

2. **Не вводить новые фреймворки**: Использовать существующие паттерны BioETL (`PipelineBase`, Pandera, Typer CLI).

3. **Быть конкретным**: Указывать пути файлов, имена модулей, команды, условия фейла.

4. **Следовать конвенциям проекта**: См. `.cursor/rules/`, `docs/styleguide/`.

5. **Все изменения проходят тесты**: `pytest tests/` должен проходить после всех правок.

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

---

**Версия документа**: 1.0.0  
**Последнее обновление**: 2025-01-29  
**Автор**: Data Acquisition Team
