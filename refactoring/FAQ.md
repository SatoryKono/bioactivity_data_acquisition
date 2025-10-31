Инвентаризация (`src/scripts/run_inventory.py`)

MUST: автоматизированный инструмент инвентаризации должен существовать. Ручной отчёт допустим единожды для бутстрапа, но артефакт инвентаризации обязан генерироваться детерминированно и повторяемо.
Артефакт: docs/requirements/PIPELINES.inventory.csv. Путь исходника: [ref: repo:src/scripts/run_inventory.py@test_refactoring_32]; конфигурация: [ref: repo:configs/inventory.yaml@test_refactoring_32].
Запись файла — атомарная: tmp → os.replace. 
Python documentation

MUST: формат вывода — CSV по RFC 4180 с фиксированной сортировкой строк (рекомендуется source, path, module). Артефакт генерируется командой `python src/scripts/run_inventory.py --check`. Поля:
source|path|module|size_kb|loc|mtime|top_symbols|imports_top|docstring_first_line|config_keys.
Обязаны соблюдаться правила кавычек/экранирования и CRLF как каноничный перевод строки. 
IETF
+1

MAY: дополнительный выход NDJSON с теми же полями и тем же порядком ключей; в CI источником истины для сравнений остаётся CSV по RFC 4180. 
IETF

MUST: интеграция в CI как проверяемый шаг спецификации PIPELINES.md (таблица источников). Цель — воспроизводимость и контроль мерджей; любые генерации файлов выполняются атомарно. 
Python documentation

Депрециированные реэкспорты

MUST: совместимость обеспечивается централизованными реэкспортами с DeprecationWarning (фазы: Warn → Deprecated → Removed). Канонический список фиксируется в [DEPRECATIONS.md](../DEPRECATIONS.md); иных параллельных списков быть не должно.
Python Enhancement Proposals (PEPs)

MUST: единая точка реэкспортов и эмиттер предупреждений, основанные на стандартном модуле warnings; политика обратной совместимости и снятия — по духу PEP 387. 
Python Enhancement Proposals (PEPs)

MUST: удаление несовместимых публичных API проводится в MAJOR версии согласно SemVer; до удаления сохраняются предупреждения и окно миграции. SHOULD: дефолтное окно депрекации — не менее двух MINOR-релизов; MAY: продление окна по change-control с фиксацией в [DEPRECATIONS.md](../DEPRECATIONS.md). Любые новые предупреждения сопровождаются обновлением версии по SemVer (инкремент MINOR) и записью в CHANGELOG.
Semantic Versioning

Форматы вывода (CSV vs Parquet)

Stage 1 (обязательный минимум): MUST — CSV как дефолтный формат вывода для всех пайплайнов. Причины: интероперабельность, простая побайтная проверка golden-файлов, формальные правила экранирования/переносов строк. 
IETF
+1

Parquet: MAY — опционально после стабилизации схем и тестов. Требования при включении: зафиксировать параметры райтера и метаданные, включая created_by, статистики и writer properties, чтобы минимизировать кроссплатформенные расхождения в бинарных футерах. Документировать выбранные опции. 
Apache Parquet
+2
DuckDB
+2

Golden-наборы, QC-отчёты и property-based тесты

SHOULD: существующие репродуцируемые golden-наборы и QC-отчёты сохранить и адаптировать под UnifiedSchema/UnifiedOutputWriter. Если текущие артефакты нерепродуцируемы или «зашумлены», допустим rebase: после унификации пересобрать, заморозить схему/порядок колонок/сортировку и далее использовать как источник истины; каноничный формат для golden — CSV по RFC 4180. 
IETF

MUST: контрактные проверки данных — Pandera (строгие схемы, проверки/домены). 
pandera.readthedocs.io
+1

SHOULD: property-based тесты на нормализацию, пагинацию и политику мерджа — на Hypothesis. 
Hypothesis
+1

CLI

MUST: Typer-команды CLI формируются из реестра `scripts.PIPELINE_COMMAND_REGISTRY` и доступны через единый вход `python -m bioetl.cli.main <pipeline>`; поддерживаемый реестр хранится в `src/scripts/__init__.py`.
Typer documentation

SHOULD: раздел FAQ ссылается на актуальный список команд из [README.md#cli-usage](../README.md#cli-usage) и перечисляет флаги общего назначения: `--config`, `--dry-run`, `--mode`.
Project README

SHOULD: приведённые примеры команд проходят дымовую проверку `python -m bioetl.cli.main list` в отдельном CI-джобе (`link-check` или `tests`).

**Фактическая структура команд:**

Все команды доступны через единый вход `python -m bioetl.cli.main`:

```bash
# Список доступных пайплайнов
python -m bioetl.cli.main list

# Запуск конкретного пайплайна
python -m bioetl.cli.main activity --config <path-to-config>
python -m bioetl.cli.main assay --config <path-to-config>
```

**Примеры вызовов:**
```bash
python -m bioetl.cli.main list
python -m bioetl.cli.main activity --config configs/pipelines/activity.yaml --dry-run --mode ci
python -m bioetl.cli.main assay --config configs/pipelines/assay.yaml
```

MAY: флаги, специфичные для пайплайнов, документируются локально, но базовые команды (`--help`, `list`, `activity`, `assay`, `target`, `document`, `testitem`) обязаны совпадать с выводом Typer.

Общие флаги CLI

SHOULD: поддерживаемые общие флаги (`--config`, `--dry-run`, `--mode`) синхронизируются с [README.md#cli-usage](../README.md#cli-usage) и документацией в исходном коде ([ref: repo:src/scripts/__init__.py@test_refactoring_32]).
TODO: дополнить раздел конкретными флагами отдельных пайплайнов по мере миграции.
Старые CLI-входы допускаются только как временные совместимые «шины» с DeprecationWarning и снимаются по графику депрекаций; финальное удаление — в ближайшем MAJOR релизе по SemVer. Процесс обновления SemVer: синхронное обновление версии в `pyproject.toml`, записи в `CHANGELOG.md` и строки в [DEPRECATIONS.md](../DEPRECATIONS.md) в рамках одного PR.
Semantic Versioning

SHOULD: единый механизм overrides: --set key=value и ENV-переменные; приоритет разрешений CLI > ENV > config. Хранить конфиг в окружении соответствует 12-Factor.
12factor.net
+1

Нормативные ссылки

Ключевые слова требований: RFC 2119. 
IETF Datatracker

CSV формат и CRLF: RFC 4180; краткая справка по CR/LF. 
IETF
+1

Атомарная запись: os.replace в стандартной библиотеке Python. 
Python documentation

HTTP Retry-After (для клиентов, если применимо): RFC 7231 и обзор. 
IETF Datatracker
+1

SemVer и правила несоответимых изменений: semver.org. 
Semantic Versioning

Pandera (валидация данных): документация. 
pandera.readthedocs.io
+1

Hypothesis (property-based): документация. 
Hypothesis
+1

Parquet: официальная спецификация метаданных и инструменты инспекции метаданных.

