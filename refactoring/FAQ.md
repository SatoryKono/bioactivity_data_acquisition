Инвентаризация (`src/scripts/run_inventory.py`)

> **Примечание:** Структура `src/bioetl/sources/` — правильная организация для внешних источников данных. Внешние источники (crossref, pubmed, openalex, semantic_scholar, iuphar, uniprot) имеют правильную структуру с подпапками (client/, request/, parser/, normalizer/, output/, pipeline.py). Для ChEMBL существует дублирование между `src/bioetl/pipelines/` (монолитные файлы) и `src/bioetl/sources/chembl/` (прокси).

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

Все команды доступны через единый вход `python -m bioetl.cli.main` и формируются на основе `scripts.PIPELINE_COMMAND_REGISTRY`:

| Команда | Описание | Конфигурация по умолчанию | Входные данные | Каталог вывода | Допустимые `--mode` |
| --- | --- | --- | --- | --- | --- |
| `activity` | ChEMBL activity data | `src/bioetl/configs/pipelines/activity.yaml` | `data/input/activity.csv` | `data/output/activity` | `default` |
| `pubchem` | Standalone PubChem enrichment dataset | `src/bioetl/configs/pipelines/pubchem.yaml` | `data/input/pubchem_lookup.csv` | `data/output/pubchem` | `default` |
| `assay` | ChEMBL assay data | `src/bioetl/configs/pipelines/assay.yaml` | `data/input/assay.csv` | `data/output/assay` | `default` |
| `target` | ChEMBL + UniProt + IUPHAR | `src/bioetl/configs/pipelines/target.yaml` | `data/input/target.csv` | `data/output/target` | `default`, `smoke` |
| `document` | ChEMBL + external sources | `src/bioetl/configs/pipelines/document.yaml` | `data/input/document.csv` | `data/output/documents` | `chembl`, `all` (по умолчанию `all`) |
| `testitem` | ChEMBL molecules + PubChem | `src/bioetl/configs/pipelines/testitem.yaml` | `data/input/testitem.csv` | `data/output/testitems` | `default` |
| `gtp_iuphar` | Guide to Pharmacology targets | `src/bioetl/configs/pipelines/iuphar.yaml` | `data/input/iuphar_targets.csv` | `data/output/iuphar` | `default` |
| `uniprot` | Standalone UniProt enrichment | `src/bioetl/configs/pipelines/uniprot.yaml` | `data/input/uniprot.csv` | `data/output/uniprot` | `default` |

**Глобальные флаги CLI**

| Флаг | Назначение | Значение по умолчанию |
| --- | --- | --- |
| `-i, --input-file PATH` | Альтернативный seed-файл для стадий extract. Если не указан, используется путь из таблицы выше. | См. реестр команд |
| `-o, --output-dir PATH` | Каталог, куда будет записан результат и QC-артефакты. Создаётся автоматически. | См. реестр команд |
| `--config PATH` | Явный путь к YAML-конфигу пайплайна. | См. реестр команд |
| `--golden PATH` | CSV-файл golden-набора для детерминированных сравнений. | `None` |
| `--limit INTEGER` | Устаревший синоним `--sample`. При совместном использовании с `--sample` значения должны совпадать. | `None` |
| `--sample INTEGER` | Ограничить количество обрабатываемых записей (smoke-тест). Требует значение ≥ 1. | `None` |
| `--fail-on-schema-drift / --allow-schema-drift` | Прерывать выполнение при обнаружении дрейфа схемы. | `--fail-on-schema-drift` |
| `--extended / --no-extended` | Управление генерацией расширенных QC-отчётов (корреляции, метрики качества). | `--no-extended` |
| `--mode TEXT` | Режим работы пайплайна. Допустимые значения указаны в таблице команд. | Значение `default` либо указанное в реестре |
| `-d, --dry-run` | Загрузить конфигурацию, не запуская пайплайн; выводит метаданные и hash конфига. | `False` |
| `-v, --verbose` | Включить подробное логирование (`UnifiedLogger` в dev-режиме). | `False` |
| `--validate-columns / --no-validate-columns` | Управление финальной проверкой колонок против требований (`ColumnValidator`). | `--validate-columns` |
| `-S, --set KEY=VALUE` | Перегрузка значений конфигурации. Флаг повторяемый; пары записываются в секцию `cli`. | `[]` |

> ⚠️ При указании `--limit` выводится предупреждение и рекомендация переходить на `--sample`.

**Примеры вызовов:**

```bash
# Список доступных пайплайнов
python -m bioetl.cli.main list

# PubChem dry-run с конфигом по умолчанию
python -m bioetl.cli.main pubchem \
  --config src/bioetl/configs/pipelines/pubchem.yaml \
  --dry-run

# Dry-run с кастомным конфигом и verbose-логированием
python -m bioetl.cli.main activity \
  --config src/bioetl/configs/pipelines/activity.yaml \
  --dry-run \
  --verbose

# Smoke-тест с ограничением выборки и расширенными QC-отчётами
python -m bioetl.cli.main target \
  --mode smoke \
  --sample 1000 \
  --extended
```

MAY: флаги, специфичные для пайплайнов, документируются локально, но базовые команды (`--help`, `list`, `activity`, `assay`, `target`, `document`, `testitem`, `gtp_iuphar`, `uniprot`) обязаны совпадать с выводом Typer.

Общие флаги CLI

SHOULD: поддерживаемые общие флаги синхронизируются с [README.md#cli-usage](../README.md#cli-usage) и документацией в исходном коде ([ref: repo:src/scripts/__init__.py@test_refactoring_32]).
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

