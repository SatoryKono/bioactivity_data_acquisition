# Отчет о результатах тестирования

**Дата:** 2025-01-27  
**Версия Python:** 3.13.7  
**Платформа:** Windows 10

## Общая статистика

- **Всего тестов:** 109
- **Пройдено:** 60 (55%)
- **Провалено:** 5 (5%)
- **Пропущено:** 0
- **Предупреждения:** 2
- **Время выполнения:** 15.12s

## Покрытие кода

- **Текущее покрытие:** 53.80%
- **Требуемое покрытие:** ≥85%
- **Недостаток:** 31.20%

### Покрытие по модулям

#### Хорошо покрытые модули (>80%)

- `src/bioetl/config/models.py` - 95%
- `src/bioetl/core/api_client.py` - 86%

#### Среднее покрытие (50-80%)

- `src/bioetl/core/logger.py` - 76%
- `src/bioetl/cli/main.py` - 75%
- `src/bioetl/config/loader.py` - 64%
- `src/bioetl/pipelines/chembl/activity.py` - 58%
- `src/bioetl/schemas/__init__.py` - 55%

#### Низкое покрытие (<50%)

- `src/bioetl/core/output.py` - 20%
- `src/bioetl/pipelines/base.py` - 27%
- `src/bioetl/core/client_factory.py` - 32%
- `src/bioetl/qc/report.py` - 32%
- `src/bioetl/config/chembl_assay.py` - 47%
- `src/bioetl/clients/chembl.py` - 24%
- `src/bioetl/qc/metrics.py` - 24%
- `src/bioetl/sources/chembl/assay/client.py` - 22%

## Провалившиеся тесты

### 1. `test_activity_command_with_limit`

**Файл:** `tests/unit/test_cli.py:209`  
**Ошибка:** `KeyError: 'config'`

**Причина:** Тест ожидает, что `ChemblActivityPipeline` вызывается с именованными аргументами (`kwargs["config"]`), но фактически вызов происходит с позиционными аргументами:

```python
pipeline = ChemblActivityPipeline(pipeline_config, run_id)
```

**Решение:** Исправить тест, использовать `call_args.args[0]` вместо `call_args.kwargs["config"]`.

### 2. `test_activity_command_with_sample`

**Файл:** `tests/unit/test_cli.py:259`  
**Ошибка:** `assert 2 == 0` (CLI возвращает код ошибки 2)

**Причина:** Тест вызывает root command (без subcommand) с опцией `--sample`, но эта опция доступна только в `activity` subcommand. Root command не имеет опции `--sample`.

**Решение:** Либо изменить тест, чтобы вызывать `activity` subcommand, либо добавить опцию `--sample` в root command.

### 3. `test_activity_command_sample_limit_mutually_exclusive`

**Файл:** `tests/unit/test_cli.py:301`  
**Ошибка:** `assert 'mutually exclusive' in result.stderr` - сообщение не найдено

**Причина:** Тест пытается вызвать root command с `--sample`, но эта опция не существует в root command, поэтому проверка взаимной исключительности не выполняется.

**Решение:** Изменить тест, чтобы вызывать `activity` subcommand, где доступны обе опции.

### 4. `test_activity_command_with_verbose_and_schema_flags`

**Файл:** `tests/unit/test_cli.py:407`  
**Ошибка:** `assert 2 == 0` (CLI возвращает код ошибки 2)

**Причина:** Тест вызывает root command с опциями `--verbose`, `--allow-schema-drift`, `--no-validate-columns`, но эти опции доступны только в `activity` subcommand.

**Решение:** Изменить тест, чтобы вызывать `activity` subcommand.

### 5. `test_init` (PipelineBase)

**Файл:** `tests/unit/test_pipeline_base.py:43`  
**Ошибка:** `AssertionError: assert 'activity_chembl' == 'activity'`

**Причина:** Тест ожидает `pipeline_code == "activity"`, но в fixture `pipeline_config_fixture` указано `name="activity_chembl"`, и `pipeline_code` берется из `config.pipeline.name`.

**Решение:** Исправить тест, изменить ожидаемое значение на `"activity_chembl"` или изменить fixture.

## Самые медленные тесты

1. `test_get_retry_exhausted` - 3.88s
2. `test_get_retry_on_500` - 2.00s
3. `test_get_timeout_error` - 1.87s
4. `test_rate_limiting` - 1.35s
5. `test_get_connection_error_with_retry` - 1.29s

## Рекомендации по исправлению

### Критические проблемы (блокируют CI)

1. **Исправить тесты CLI:**
   - Исправить проверку аргументов в `test_activity_command_with_limit` (использовать `args` вместо `kwargs`)
   - Изменить тесты, использующие `--sample`, `--verbose`, `--allow-schema-drift`, `--no-validate-columns`, чтобы вызывать `activity` subcommand вместо root command
   - Исправить ожидаемое значение `pipeline_code` в `test_init`

2. **Увеличить покрытие кода до ≥85%:**
   - Добавить тесты для `src/bioetl/core/output.py` (текущее покрытие 20%)
   - Добавить тесты для `src/bioetl/pipelines/base.py` (текущее покрытие 27%)
   - Добавить тесты для `src/bioetl/core/client_factory.py` (текущее покрытие 32%)
   - Добавить тесты для модулей с низким покрытием

### Приоритетные улучшения

1. **Оптимизация медленных тестов:**
   - Рассмотреть использование моков для сетевых вызовов в тестах retry/timeout
   - Уменьшить таймауты в тестах для ускорения выполнения

2. **Документация тестов:**
   - Убедиться, что все тесты имеют понятные docstrings
   - Добавить комментарии для сложных тестов

## Выводы

Проект имеет хорошую основу тестирования с 60 успешными тестами, но есть несколько критических проблем:

1. **5 провалившихся тестов** требуют исправления - все связаны с CLI и различиями между root command и subcommand
2. **Покрытие кода 53.80%** значительно ниже требуемого порога 85%
3. **Некоторые модули имеют очень низкое покрытие** и требуют дополнительных тестов

После исправления провалившихся тестов и увеличения покрытия кода проект будет соответствовать требованиям CI.
