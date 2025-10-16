# Project Requirements

## Contents

- [1. Architecture Overview](#1-architecture-overview)

- [2. Modules & Dependencies](#2-modules--dependencies)

- [3. CLI Reference](#3-cli-reference)

- [4. Clients (HTTP) Spec](#4-clients-http-spec)

- [5. Pipelines](#5-pipelines)

  - [5.1 Target](#51-target)

  - [5.2 Document](#52-document)

  - [5.3 Assay](#53-assay)

  - [5.4 Activity](#54-activity)

- [6. Schemas](#6-schemas)

- [7. IO & Config](#7-io--config)

- [8. Reliability](#8-reliability)

- [9. Tests](#9-tests)

- [10. CI & Dev Checks](#10-ci--dev-checks)

- [11. Risks & Improvements](#11-risks--improvements)

- [12. Applications & Артефакты](#12-applications--артефакты)

- [13. Сводный чек-лист проверки](#13-сводный-чек-лист-проверки)

- [14. История версий документа](#14-история-версий-документа)

- [Summary](#summary)

## 1. Architecture Overview

```

flowchart LR
    subgraph External*APIs
        Chembl["ChEMBL REST"]
        PubChem["PubChem PUG"]
        UniProt["UniProt REST"]
        CrossRef["CrossRef Works"]
        OpenAlex["OpenAlex Works"]
    end

    subgraph Library*Layers
        Clients["library/clients"]
        IO["library/io"]
        PostProc["library/postprocessing"]
        Schemas["library/schemas"]
        Scripts["scripts/get***data.py"]
    end

    Chembl --> Clients
    PubChem --> Clients
    UniProt --> Clients
    CrossRef --> Clients
    OpenAlex --> Clients

    Clients --> IO
    IO --> PostProc
    PostProc --> Schemas
    Schemas --> Scripts

    Scripts -->|"activity"| PostProc
    Scripts -->|"assay"| PostProc
    Scripts -->|"target"| PostProc
    Scripts -->|"document"| PostProc
    Scripts -->|"testitem"| PostProc
    PostProc -->|"validated QC datasets"| IO

```

### Таблица директорий верхнего уровня

| Имя | Путь/Модуль | Краткое назначение | Зависимости/Входы | Артефакты/Выходы
|

| --- | --- | --- | --- | --- |

| scripts | scripts/ | CLI-обёртки ETL и утилиты, совместимые с
legacy-пайплайнами | library.cli, postprocessing, io | CSV/QA бандлы, метаданные
|

| clients | library/clients/ | HTTP-клиенты для ChEMBL, PubChem, UniProt,
CrossRef, OpenAlex | requests, library.utils.retry | JSON-пэйлоады |

| io | library/io/ | запись CSV, метаданные, конфиги | pandas, metadata writer,
config | output.* CSV, .meta.yaml |

| postprocessing | library/postprocessing/ | модульные ETL-стадии на pandas |
clients, schemas, utils | нормализованные DataFrame |

| schemas | library/schemas/ | Pandera-схемы таблиц | pandera | DataFrame после
validate |

| utils | library/utils/ | логирование, ретраи, QC | logging, backoff |
структурированные логи, retry-декораторы |

| tests | tests/ | pytest-покрытие (unit/integration/cli) | pytest, pandas |
отчёты pytest, golden CSV |

| data/output | data/output/ | каталог для стандартных артефактов |
output*writer | output.<table>**.csv |

| dictionary | dictionary/ | справочники для обогащения | CSV dictionary |
lookup-данные |

| docs | docs/ | руководства, протоколы | n/a | статичная документация |

### Сводная таблица CLI-скриптов`scripts/get***data.py`

| Имя | Путь/Модуль | Краткое назначение | Зависимости/Входы | Артефакты/Выходы
|

| --- | --- | --- | --- | --- |

| Activity | scripts/get*activity*data.py | Загрузка активностей ChEMBL,
постобработка и QC | идентификаторы, ChEMBL API, postprocessing.activities |
output.activity*<date>.csv, **quality*report*table.csv,
**data*correlation*report*table.csv, .meta.yaml |

| Assay | scripts/get*assay*data.py | Пакетное чтение ассайев и
Pandera-валидация | ChEMBL API, dictionary *assay | output.assay*<date>.csv и QC
отчёты |

| Target | scripts/get*target*data.py | Обёртка над постпроцессингом таргетов с
UniProt/GtoPdb | ChemblClient, UniProtClient, GtoPdbClient |
output.target*<date>.csv и QC отчёты |

| Document | scripts/get*document*data.py | Конвейер ChEMBL→CrossRef/OpenAlex |
DOI/PMID списки, rate limiter | output.document*<date>.csv и QC отчёты |

| Testitem | scripts/get*testitem*data.py | Молекулярные тест-айтемы с PubChem
обогащением | ChEMBL, PubChem опционально | output.testitem*<date>.csv и QC
отчёты |

> **Важно.** Легаси-скрипты в`scripts/get***data.py`теперь выступают только
как обёртки вокруг
> канонического Typer-приложения`bioactivity.cli`и при запуске выводят`DeprecationWarning`.
> Для новых сценариев используйте точку входа `bioactivity-data-acquisition
pipeline --config ...`,
> а настройки конкретных пайплайнов задавайте через YAML/`--set`(см.`configs/config.yaml`и
>`configs/pipelines.toml`).

#### Проверка

```

bioactivity-data-acquisition pipeline --config configs/config.yaml --set
logging.level=DEBUG --set postprocess.qc.enabled=true

```

## 2. Modules & Dependencies

### Обязанности пакетов`library/*`

| Имя | Путь/Модуль | Краткое назначение | Зависимости/Входы | Артефакты/Выходы
|

| --- | --- | --- | --- | --- |

| logging | library/utils/logging.py | Структурированное логирование с
контекстом стадий | logging, ContextVar | форматированные записи |

| retry | library/utils/retry.py | backoff-декораторы над requests | backoff /
fallback, requests | надёжные вызовы |

| output*writer | library/io/output*writer.py | Детерминированная запись CSV+QC
| pandas, csv*utils | стандартный бандл CSV |

| metadata*writer | library/io/metadata*writer.py | сериализация .meta.yaml |
metadata*writer общ., RunContext | YAML sidecar |

| config*loader | library/io/config*loader.py | чтение config.yaml с ENV
override | yaml, os | Config dataclass |

| clients.* | см. раздел 4 | API-клиенты | requests, retry | JSON |

| postprocessing.* | см. раздел 5 | ETL-стадии на pandas | clients, schemas |
нормализованные DataFrame |

| schemas.* | см. раздел 6 | Pandera DataFrameSchema | pandera, pandas |
валидация |

| utils/qc*report | library/utils/qc*report.py | Формирование QC/корреляций |
TableQualityProfiler | QC DataFrame |

### Граф зависимостей

```

graph TD
    subgraph utils
        Logging
        Retry
        QC
    end
    Clients --> Retry
    Clients --> Logging
    IO --> Logging
    IO --> QC
    Postprocessing --> Clients
    Postprocessing --> IO
    Postprocessing --> Schemas
    Schemas --> Pandera
    Scripts --> Postprocessing
    Scripts --> IO

```

### Внешние зависимости

| Пакет | Версия | Назначение |

| --- | --- | --- |

| numpy | 1.26.4 / 2.1.3 | числовые операции для pandas |

| pandas | 2.1.4 / 2.2.3 | ETL, DataFrame |

| requests | 2.32.3 | HTTP-клиенты |

| PyYAML | 6.0.2 | конфиги |

| openpyxl | 3.1.5 | экспорт Excel (утилиты) |

| pyarrow | 18.1.0 | паркет/arrow I/O |

| jsonschema | 4.23.0 | валидация JSON (config/schema) |

| pandera | 0.20.3 | DataFrameSchema |

| cachetools | 5.3.3 | кэширование |

| pydantic | 2.8.2 | конфиги/валидации |

| backoff | 2.2.1 (dev) | экспоненциальный retry |

#### Проверка

```

python -c "import pandas, requests, pandera, backoff"
pip list | grep -E 'pandas|requests|pandera|backoff'

```

## 3. CLI Reference

### Параметры`get*activity*data.py`

| Флаг | Тип/Значения | По умолчанию | Назначение | Обязательность |

| --- | --- | --- | --- | --- |

| --input | Path | activity.csv | входной CSV с ID | нет |

| --limit | int | ∞ | ограничение ID | нет |

| --dry-run | bool | false | проверка без записи | нет |

| --workers | int | 1 | потоки fetch | нет |

| --postprocess/--no-postprocess | bool | off | включить постпроцессинг | нет |

| --output-dir | Path | data/output | каталог артефактов | нет |

| --date-tag | str | авто (YYYYMMDD) | тег выпусков | нет |

Возвраты: 0 при успешной обработке, 1 при ошибках валидации (см. unit-тест на
отрицательный limit).

Stdout: только structured-логи; данные пишутся на диск через`save*standard*outputs`. Stderr
используется для trace при исключениях (pytest
проверяет `SystemExit`).

### Параметры `get*assay*data.py`

Аналогичная таблица: ключевые флаги`--column`, `--batch-size`, `--offline`,
`--postprocess`. Все необязательные.

### Параметры `get*document*data.py`

Исторические флаги (`--mode`, `--crossref-rps`, `--openalex-rps`и т. д.) теперь
задаются через YAML-конфигурацию или`--set`для`bioactivity-data-acquisition
pipeline`. При запуске легаси-обёртки параметры прокидываются в общий конфиг.

### Параметры `get*target*data.py`

Флаги`--limit`, `--date-tag`, `--output-dir`также переехали в конфигурацию.
Используйте профиль/секцию в`configs/pipelines.toml` либо CLI-override (`--set
postprocess.qc.enabled=false`и т. п.).

### Параметры`get*testitem*data.py`

Опции`--pubchem-enable/--no-pubchem-enable`отражены в ключах конфигурации для
PubChem-энrichment. Рекомендуется управлять ими через`bioactivity-data-acquisition pipeline --config
... --set
pipelines.testitem.include*pubchem=false`(или правку YAML).

### Эталонные команды

```

bioactivity-data-acquisition pipeline --config configs/config.yaml \
  --set postprocess.qc.enabled=true \
  --set postprocess.reporting.include*timestamp=true

```

#### Проверка

```

bioactivity-data-acquisition --help
bioactivity-data-acquisition pipeline --help

```

## 4. Clients (HTTP) Spec

| Клиент | Конечные точки | Таймауты/Retry | Формат ответа | Обработка ошибок |

| --- | --- | --- | --- | --- |

| ChemblClient | /molecule/{id}, /target.json | timeout=10s, max*tries=3
(override), backoff expo | JSON → dict |`raise*for*status`, structured логи,
retry warn/error |

| PubChemClient | /compound/cid/{cid}/JSON | аналогично | JSON | retries, лог
`pubchem*request`|

| UniProtClient | /uniprotkb/{accession}.json | аналогично | JSON | retries, лог`uniprot*request`|

| CrossrefClient | /works/{doi} | аналогично | JSON | retries, лог`crossref*request`|

### Нормализация → DataFrame

Document pipeline:`pd.DataFrame.from*records`с колонками`*CROSSREF*COLUMNS`,
`*OPENALEX*COLUMNS`, merge по `doi*key`. Combine-first для `title`/`doc*type`,
защита от перезаписи.

Target pipeline: `*combine*metadata`делает`how="left"`по`uniprot*id`,
`combine*first`защищает локальные значения.

### Стабильность

| Клиент | 429/5xx | Лимиты |

| --- | --- | --- |

| ChEMBL | backoff retry с логами`retry/giveup`| страница ≤200, offset
прогресс |

| CrossRef | rate limiter через`get*limiter`, запись ошибок в `crossref*error`| RPS из config |

| OpenAlex | rate limiter аналогичен, ошибки →`openalex*error`| RPS из config
|

| UniProt/GtoPdb | retry, ошибки логируются и пропускаются | — |

#### Проверка

```

## Псевдо-запросы

python - <<'PY'
from library.clients.chembl*client import ChemblClient
print(ChemblClient().*request*json(endpoint="target.json", params={"limit":1},
timeout=0.1)) # мок через monkeypatch requests.Session.get
PY

```

## 5. Pipelines

### 5.1 Target

```

flowchart LR
    Fetch["fetch*normalize*target"] --> Normalize["*normalise*targets"]
    Normalize --> Enrich1["*load*uniprot*metadata"]
    Normalize --> Enrich2["*load*gtopdb*metadata"]
    Enrich1 --> Merge["enrich*target*metadata (combine*first)"]
    Enrich2 --> Merge
    Merge --> Validate["TargetSchema.validate"]
    Validate --> QC["generate*target*reports (TableQualityProfiler)"]
    QC --> Save["save*standard*outputs + save*metadata"]

```

#### Таблица стадий

| Стадия | Входные колонки | Нормализация | Итоговые колонки |

| --- | --- | --- | --- |

| *normalise*targets | raw JSON target*chembl*id, pref*name, components |
trim/типизация StringDtype, skip пустые ID | *TARGET*COLUMNS (9 колонок) |

| UniProt merge | uniprot*id, UniProt payload | combine*first protein*family,
synonyms | добавленные столбцы |

| GtoPdb merge | target*class | combine*first | сохранение curator значений |

| Validate | Pandera TargetSchema | coerce=True | строковые типы |

| QC | TableQualityProfiler | отчёты + summary | QC таблицы |

Merge стратегия –`merge(..., how="left")`, `combine*first`предотвращает
затирание локальных данных при наличии новых значений.`synonyms`нормализуются
через`*normalise*synonym*list`.

Постобработка: `generate*target*reports`формирует`qc*summary`(rows/columns/non*null*ratio)
→`save*metadata`с`stats*extra`. QC отчёты
именуются стандартно.

`.meta.yaml`поля:`table`, `parameters`, `sources`, `outputs`, `qc*summary`,
`stats`. Дополнительно `generated*at`, `stats.records`и т.п.

#### Проверка

Запуск выполняется через канонический CLI, например:

```

bioactivity-data-acquisition pipeline --config configs/target.yaml

```

где`configs/target.yaml`— производная от`configs/config.yaml`конфигурация с
нужными limit/merge-настройками. Легаси-скрипт`scripts/get*target*data.py`сохраняется лишь как
обёртка и выводит предупреждение.

### 5.2 Document

```

flowchart TD
FetchChembl["fetch*chembl*documents"] --> Normalize["normalize*document*frame"]
    Normalize --> FetchCrossRef["fetch*crossref*metadata"]
    Normalize --> FetchOpenAlex["fetch*openalex*metadata"]
    FetchCrossRef --> Merge["merge*document*metadata"]
    FetchOpenAlex --> Merge
    Merge --> Validate["validate*document*frame"]
    Validate --> QC["TableQualityProfiler + build*reports"]
    QC --> Save["save*standard*outputs + save*metadata"]

```

#### Колонки/правила

| Стадия | Правила | Выход |

| --- | --- | --- |

| Fetch | пагинация limit/offset,`*update*limit*parameter`| DataFrame`*CHEMBL*DOCUMENT*COLUMNS`|

| Normalize | Trim,`clean*doi*value`, numeric PMID→string | добавляется
`doi*key`|

| CrossRef | Rate limiter, ошибки →`crossref*error`|`*CROSSREF*COLUMNS`|

| OpenAlex | Rate limiter, DOI нормализация |`*OPENALEX*COLUMNS`|

| Merge | left join,`combine*first`для`title/doc*type`| enriched DataFrame |

| Validate | Pandera schema strict=False | enriched frame |

| QC |`profiler.consume`+`build*reports`| quality + correlation |

Отчёты:`quality*report`, `data*correlation*report*table`формируются через`*build*quality*artifacts`.

#### Проверка

```

bioactivity-data-acquisition pipeline --config configs/document.yaml

```

Создайте профайл`configs/document.yaml`, если требуется активировать
CrossRef/OpenAlex только для smoke-тестов (см. `configs/pipelines.toml`).

### 5.3 Assay

```

flowchart LR
    Fetcher["AssayFetcher.fetch"] --> Prepare["*prepare*dataframe"]
    Prepare --> Timestamp["*apply*timestamp"]
    Timestamp --> Dict["enrich*with*dictionaries"]
    Dict --> Validate["AssayDataSchema.validate"]
    Validate --> Save["save*standard*outputs + save*metadata"]

```

#### Стадия → Описание

| Стадия | Описание | Итог |

| --- | --- | --- |

| AssayFetcher | запрос assay.json батчами, retry | список dict |

| *prepare*dataframe | заполнение NA, сортировка колонок, cast | DataFrame |

| *apply*timestamp | derive UTC timestamp, year Int64 | обогащённый DataFrame |

| enrich*with*dictionaries | merge dictionary CSV (combine*first) | DataFrame с
lookup |

| Validate | Pandera schema | строгая структура |

#### Проверка

```

bioactivity-data-acquisition pipeline --config configs/assay.yaml

```

Конфигурация`configs/assay.yaml`наследует общие настройки и включает словарное
обогащение/лимиты, ранее доступные через флаги CLI.

### 5.4 Activity

```

flowchart LR
Normalize["normalize*activity*records"] --> Quality["enrich*activity*quality"]
    Quality --> Finalize["finalize*activity*records"]
    Finalize --> Run["run*activity*pipeline (config-driven)"]
    Run --> QC["PipelineRunMetrics + QC hooks"]
    QC --> Save["save*standard*outputs + save*metadata"]

```

#### Стадия → Правила

| Стадия | Правила | Выход |

| --- | --- | --- |

| Normalize | lower/trim, uppercase relation/units | cleaned DataFrame |

| Quality | детерминированный flag по comment | bool column`quality*flag`|

| Finalize | cast ID to Int64, Pandera schema | валидированный DataFrame |

| Run pipeline | config-driven steps + metrics | Dataset + PipelineRunMetrics |

#### Проверка

```

bioactivity-data-acquisition pipeline --config configs/activity.yaml

```

Файл`configs/activity.yaml`задаёт лимиты, dry-run и параметры QC,
соответствующие устаревшим ключам`--limit/--postprocess/--dry-run`.

## 6. Schemas

| Схема | Путь | Поля (тип, nullable) | Поведение |

| --- | --- | --- | --- |

| TargetSchema | library/schemas/target*schema.py | target*chembl*id..synonyms
(StringDtype, часть nullable) | strict=True, lazy validation |

| DocumentSchema | library/schemas/document*schema.py | base+CrossRef+OpenAlex,
строки nullable | strict=False (разрешены дополнительные столбцы) |

| AssayDataSchema | library/schemas/assay*schema.py | assay*chembl*id
(non-null), даты UTC | lazy, nullable fields |

| ActivitySchema | library/schemas/activity.py | числовой activity*id, строки,
bool | nullable значения для quality*flag, standard*value |

### Строгие/мягкие поля

| Схема | Строгие поля | Мягкие поля | Действие при несоответствии |

| --- | --- | --- | --- |

| Target | все 9 | — | Pandera SchemaErrors |

| Document | base strict, доп. поля optional | CrossRef/OpenAlex nullable |
ошибка при отсутствующих ключевых столбцах |

| Assay | assay*chembl*id обязательный | остальные nullable | SchemaErrors
(lazy=true) |

| Activity | activity*id обязателен | quality*flag nullable | SchemaErrors |

Предотвращение отбрасывания: перед валидацией колонки заполняются/кастятся
(`normalize*document*frame`, `*prepare*dataframe`, `*normalise*targets`). Tests
покрывают порядок (`test*output*writer`, `test*document*normalize`).

#### Проверка

```

python - <<'PY'
import pandas as pd
from library.schemas.target*schema import TargetSchema
df =
pd.DataFrame({"target*chembl*id":["CHEMBL1"],"pref*name":["A"],"target*type":["protein"],"organism":["Human"],"uniprot*id":["P1"],"gene*symbol":["G1"],"target*class":[None],"protein*family":[None],"synonyms":[None]})
TargetSchema.validate(df)
PY

```

Ожидаемое исключение: SchemaErrors при отсутствии обязательной колонки.

## 7. IO & Config

`save*standard*outputs(df*main, df*corr, df*qc, table*name, date*tag, ...)`—
имя`output.<table>*<date>.csv`, QC отчёты c суффиксами
`*quality*report*table.csv`, `*data*correlation*report*table.csv`.
`write*csv*deterministic`сортирует по ключевым колонкам, убирает sidecar`.meta.yaml.lock`. `na*rep`
управляется pandas default (пустая строка→`<NA>`).

`metadata*writer.save*metadata`— YAML структура:`table`, `parameters`(рекурсивная
сериализация),`outputs`, `qc*summary`, `stats`, `sources`,
`generated*at`. Берёт RunContext или текущую UTC. Артефакты по имени, если не
переданы — дефолтный тройной список.

`config*loader.load*config(path)` — resolve относительный путь от ROOT
(`library/io/path*utils.py`), ENV override `CHEMBL*DA**SECTION**KEY`, YAML
coercion. ROOT=`Path(**file**).resolve().parents[2]`. `OUTPUT*DIR` создаётся
автоматически.

Политика путей: все относительные (`Path(...)/parents[2]`), запрет абсолютных
при записи.

#### Проверка

```

python - <<'PY'
from pathlib import Path
import pandas as pd
from library.io.output*writer import save*standard*outputs
df = pd.DataFrame({"id":[1]})
artifacts = save*standard*outputs(df, pd.DataFrame(), pd.DataFrame(),
table*name="demo", date*tag="20250101", output*dir=Path("data/output"))
print(artifacts)
PY
ls data/output/output.demo*20250101*

```

## 8. Reliability

Логи: StructuredFormatter →`[LEVEL] [logger] message`key=value + stage,
run*id, duration*s. Context менеджеры`log*context`, `log*stage`.

Ретраи: `with*retry`(backoff expo, full jitter, max*tries>=1). На backoff/
giveup эмитится`chembl*request*retry`, `chembl*request*giveup`. Таймаут по
умолчанию 10s. Ошибка — raise.

Кэш/lock: deterministic writer удаляет `.meta.yaml`и`.lock`после записи;
cleanup скрипт чистит`*.lock`, `**intermediate*`, pytest caches. Fail-fast:
ошибки в Pandera пробрасываются → `SystemExit=1`(unit tests).

Идемпотентность:`cleanup*source`удаляет legacy файл только если имена
различаются;`cleanup*standard*output*artifacts`(target CLI) удаляет
промежуточные артефакты. Retry на HTTP даёт устойчивость.

#### Проверка

```

## В конфигурации activity-пайплайна задайте limit=-1 и запустите канонический CLI,

## ожидая `SystemExit`:

bioactivity-data-acquisition pipeline --config configs/activity.yaml
python scripts/cleanup*project.py --dry-run --retention-days 0

```

## 9. Tests

### Структура

| Категория | Путь | Покрытие |

| --- | --- | --- |

| Unit | tests/library, tests/unit | utils, schemas, metadata |

| CLI | tests/cli | smoke CLI (exit codes, артефакты) |

| Integration/Postprocess | tests/postprocess/test*activity*pipeline.py |
end-to-end activity steps |

| Helpers | tests/helpers, fixtures | mocks HTTP, tmp*path |

Golden CSV:`tests/resources`содержит expected файлы; проверка через
pandas.`test*output*writer`гарантирует порядок колонок и отсутствие sidecar.

Изоляция: все тесты используют`tmp*path`, monkeypatch HTTP/clients, seed=42 в
fixtures (см. `tests/conftest.py`).

#### Проверка

```

pytest -q --disable-warnings
pytest tests/cli/test*get*target*data.py -q

```

## 10. CI & Dev Checks

GitHub Actions`ci.yml`: Python 3.11, install requirements-lock, `pip check`,
затем `ruff check .`, `mypy --config-file pyproject.toml`, `pytest --cov`, отчёт
coverage артефакт.

Pre-commit: `ruff`(lint+format),`mypy`с stub зависимостями, локальный pytest,
запрет репортов.

Локальный чек-лист разработчика:

-`ruff check .`

-`mypy library/ scripts/`

-`pytest -q --disable-warnings`

-`pre-commit run --all-files`

## 11. Risks & Improvements

| Файл/Строка | Риск | Влияние | Рекомендация | Сложность |

| --- | --- | --- | --- | --- |

| library/postprocessing/document/steps.py (многократные merges) | отсутствие
unit-тестов на combine*first конфликтов | Потенциальная потеря CrossRef данных |
добавить тесты на конфликт DOI и doc*type | M |

| scripts/get*document*data.py (огромный CLI) | перегруженный интерфейс, сложно
поддерживать | Ошибки конфигурации | вынести профили конфигов в YAML-профиль | L
|

| library/postprocessing/target/steps.py (цикл API без rate-limit) | нет rate
limiter → риск 429 | Прерывание выгрузки | интегрировать limiter аналогично
document | M |

| tests/cli/test*get*activity*data.py (моки сложны) | сложный monkeypatch →
flaky | Ложно отрицательные тесты | выделить fixture для pipeline runner | M |

| scripts/cleanup*project.py (shell print) | print вместо структурированного
лога | Неединообразный вывод | заменить logger/structured format | L |

### Приоритетный план (10 пунктов)

1. Добавить rate limiter в`fetch*normalize*target`для UniProt/GtoPdb.
2. Покрыть merge conflict tests (CrossRef/OpenAlex).
3. Ввести конфиг-профили для document CLI.
4. Экстрагировать общие CLI-парсер секции в helper.
5. Добавить CLI тесты для`get*testitem*data`.
6. Автоматизировать mock HTTP fixtures (`responses`).
7. Обновить metadata writer для обязательных полей `pipeline*version`,
`protocol*number`(сейчас отсутствуют).
8. Включить`ruff format`в CI (прогон уже есть в pre-commit).
9. Добавить smoke тест`--dry-run`для всех CLI (pytest).
10. Обновить docs/ru + docs/en синхронно с новой документацией.

Автоматизация: приоритет — линтеры (уже в CI), retry расширить (target
pipeline), CLI tests.

## 12. Applications & Артефакты

### Новые markdown-файлы (отсутствуют, к внедрению)

| Файл | Статус | Структура | Ссылки |

| --- | --- | --- | --- |

| docs/ARCHITECTURE.md | отсутствует | TOC, разделы: Обзор, Dataflow (mermaid),
Модули, Интеграции, Команды воспроизведения | ссылки на разделы
(#architecture-overview) |

| docs/MODULES.md | отсутствует | каталог API модулей, таблицы Responsibilities,
Commands | ссылается на Clients/IO/Schemas |

| docs/CLI.md | отсутствует | таблицы аргументов, примеры, exit codes |
ссылается на Scripts |

| docs/SCHEMAS.md | отсутствует | перечень полей Pandera, nullable policy |
ссылки на tests |

| docs/DATAFLOW.md | отсутствует | граф ETL стадий, inputs/outputs | ссылается
на pipelines |

| docs/RELIABILITY.md | отсутствует | retry матрица, log формат, cleanup
процедуры | ссыль на cleanup*project |

| docs/TESTING.md | отсутствует | стратегия pytest, фикстуры, команды |
ссылается на CI |

| docs/GLOSSARY.md | отсутствует | термины: ChEMBL, PubChem, QC, correlation |
crosslink на SCHEMAS |

Каждый файл: добавить мини-оглавление (- [ ]), описать команды в конце.

### Сводная таблица артефактов

| Имя файла | Когда создаётся | Downstream использование |

| --- | --- | --- |

| output.<table>*<date>.csv |`save*standard*outputs`в конце пайплайна | вход в
BI/аналитику |

| output.<table>*<date>*quality*report*table.csv | QC writer | аудит качества |

| output.<table>*<date>*data*correlation*report*table.csv | QC writer |
корреляционный анализ |

| output.<table>*<date>.meta.yaml |`metadata*writer.save*metadata`|
lineage/протокол |

| **data*correlation*report*table.csv |`build*reports*from*profiler`|
downstream QC dashboards |

#### Проверка

```

ls docs

## убедиться в наличии новых файлов после внедрения

```

## 13. Сводный чек-лист проверки

1. Настроить окружение (Python 3.11, зависимости из requirements-lock.txt).
2. Выполнить dry-run всех CLI с`--limit 10`и`--output-dir tmp`.
3. Проверить наличие стандартных артефактов и `.meta.yaml`.
4. Просмотреть structured-логи на ошибки.
5. Запустить `pytest -q --disable-warnings`.
6. Выполнить `ruff check .`и`mypy --config-file pyproject.toml`.
7. Проверить `cleanup_project.py --dry-run`.
8. Сверить QC отчёты (пустые ⇒ нет данных).
9. Убедиться в заполнении ключевых полей `.meta.yaml`.
10. Обновить документацию (файлы из раздела 12) и приложить к релизу.

## 14. История версий документа

| Version | Date | Changes |

| --- | --- | --- |

| 1.0.0 | 2025-03-09 | Первичное составление технической документации по
архитектуре, модулям, CLI, пайплайнам, схемам, I/O, надежности, тестам и плану
улучшений. |

## Summary

Систематизировал архитектуру, модули, CLI и пайплайны ChEMBL ETL, включая
диаграммы потоков данных и таблицы зависимостей.

Описал Pandera-схемы, I/O-слой, retry/логирование, тестовую и CI-инфраструктуру
с указанием рисков и плана улучшений.
