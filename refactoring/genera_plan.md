# 1. Область действия и принципы (@test_refactoring_32)

> **Примечание:** Структура `src/bioetl/sources/` — правильная организация для внешних источников данных. Внешние источники (crossref, pubmed, openalex, semantic_scholar, iuphar, uniprot) имеют правильную структуру с подпапками (client/, request/, pagination/, parser/, normalizer/, schema/, merge/, output/, pipeline.py). Для ChEMBL существует дублирование между `src/bioetl/pipelines/` (монолитные файлы) и `src/bioetl/sources/chembl/` (прокси).

Объект: пайплайны загрузки и нормализации данных из внешних источников, размещённые в `src/bioetl/sources/<source>/` с общими слоями в `src/bioetl/core/`.

**Принцип единообразия**: для каждого источника существует ровно один публичный пайплайн, собранный из минимального набора повторно используемых модулей с единым контрактом API. Это обязательство является проверяемым требованием (см. §2).

**Детерминизм вывода**: порядок колонок/ключей и строк должен быть стабилен; запись артефактов осуществляется атомарно: temp-файл → `os.replace`. Операция замены должна выполняться как единое атомарное действие на целевом файловом объекте.
[Python documentation @test_refactoring_32](https://docs.python.org/3/library/os.html#os.replace)

**Строгая валидация**:
- данные валидируются Pandera-схемами на уровне DataFrame/Series;
- конфигурации валидируются Pydantic-совместимыми моделями с автогенерацией JSON Schema.
[pandera.readthedocs.io @test_refactoring_32](https://pandera.readthedocs.io/)
[pandera.readthedocs.io @test_refactoring_32](https://pandera.readthedocs.io/)

**Backoff/Retry**: клиенты обязаны уважать HTTP Retry-After и корректно обрабатывать 429/503 при троттлинге и деградации сервиса.
[datatracker.ietf.org @test_refactoring_32](https://datatracker.ietf.org/doc/html/rfc7231#section-7.1.3)



## Источники истины (@test_refactoring_32)

- [ref: repo:docs/requirements/PIPELINES.inventory.csv@test_refactoring_32] — актуальный CSV-слепок пайплайнов.
- [ref: repo:docs/requirements/PIPELINES.inventory.clusters.md@test_refactoring_32] — отчёт о кластеризации компонентов.
- [ref: repo:configs/inventory.yaml@test_refactoring_32] — конфигурация генератора инвентаризации.
- [ref: repo:src/scripts/run_inventory.py@test_refactoring_32] — CLI для генерации и проверки артефактов.
- [ref: repo:tests/unit/test_inventory.py@test_refactoring_32] — тесты для слепка инвентаризации.

# 2. Цели унификации (проверяемые требования) (@test_refactoring_32)

**MUST**: для каждого источника существует ровно один публичный пайплайн с детерминированным выводом и строгой валидацией.

**MUST**: общая логика вынесена в базовые абстракции `core/`; различия источников выражены стратегиями/параметрами.

**MUST**: конфиги валидируются единой схемой; нераспознанные/конфликтные ключи запрещены.

**SHOULD**: модульные тесты на каждый слой и интеграционные тесты на пайплайн.

**MUST NOT**: дублирующие реализации client/adapter/normalizer/schema/output за пределами `core/` и шаблона источника.


# 3. Базовая структура каждого пайплайна (@test_refactoring_32)

**Директория**: `src/bioetl/sources/<source>/`

- `client/`      # HTTP-клиент: ретраи/бэкофф, rate-limit, Retry-After, телеметрия
- `request/`     # RequestBuilder: paths/templates, params, auth, headers
- `pagination/`  # Стратегии: cursor | page | offset | token | datewindow
- `parser/`      # JSON/CSV/XML/NDJSON парсинг, streaming-safe
- `normalizer/`  # Нормализация ID/единиц/онтологий; дедупликация
- `schema/`      # Pandera-схемы (вход/выход), строгие типы/домены
- `merge/`       # MergePolicy: ключи, приоритеты источников, conflict-resolution
- `output/`      # Writer: детерминизм, атомарная запись, meta.yaml
- `pipeline.py`  # Реализация PipelineBase с хуками

Конфигурация источника описывается файлом `src/bioetl/configs/pipelines/<source>.yaml` (MUST) с включениями из `src/bioetl/configs/includes/` при необходимости.

**Общие слои**: [ref: repo:src/bioetl/core/@test_refactoring_32].

# 4. Единый публичный API (@test_refactoring_32)

Пайплайн обязан реализовывать следующий контракт:

```python
# [ref: repo:src/bioetl/pipelines/base.py@test_refactoring_32]
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import pandas as pd

from bioetl.core.output_writer import OutputArtifacts


class PipelineBase(ABC):
    @abstractmethod
    def extract(self, *args: Any, **kwargs: Any) -> pd.DataFrame: ...

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame: ...

    @abstractmethod
    def validate(self, df: pd.DataFrame) -> pd.DataFrame: ...

    def export(
        self,
        df: pd.DataFrame,
        output_path: Path,
        extended: bool = False,
    ) -> OutputArtifacts: ...

    def run(
        self,
        output_path: Path,
        extended: bool = False,
        *args: Any,
        **kwargs: Any,
    ) -> OutputArtifacts: ...
```

> **Табличный контракт:** все стадии передают данные в виде `pd.DataFrame` и обязаны возвращать DataFrame, совместимый с Pandera-схемами и `PipelineBase.run()`. Общие определения и поведение описаны в `PipelineBase` (`src/bioetl/pipelines/base.py`).

**Обязательные общие компоненты**:

- **Writer** с атомарной записью и стабильной сортировкой:
 [ref: repo:src/bioetl/core/output_writer.py@test_refactoring_32],
 [ref: repo:src/bioetl/core/io_atomic.py@test_refactoring_32].
 [Python documentation @test_refactoring_32](https://docs.python.org/3/library/os.html#os.replace)

- **Логгер** с run_id и структурированными событиями:
 [ref: repo:src/bioetl/core/logger.py@test_refactoring_32].

# 5. Таблица источников и целевой модульный состав (@test_refactoring_32)

| source | target_layout | public_api | config_keys (MUST) | merge_policy (MUST) | tests_required (SHOULD) | risks |
|--------|---------------|------------|-------------------|---------------------|-------------------------|-------|
| Crossref | `src/bioetl/sources/crossref/{client,request,pagination,parser,normalizer,schema,merge,output,pipeline.py}` | `from bioetl.sources.crossref import CrossrefPipeline` | `api_base, mailto, rate_limit_rps, retries, backoff, query, filter, rows, select, cursor` | первичный ключ DOI; при конфликте метаданных приоритет Crossref → PubMed | unit: client/pagination/parser/normalizer/schema; integ: smoke + golden (articles.jsonl, refs.jsonl) | нестабильные поля авторов |
| PubChem | `src/bioetl/sources/pubchem/...` | `from bioetl.sources.pubchem import PubChemPipeline` | `` `api_base, namespace(inchikey` `` `cid), chunk_size, retries, backoff` | ключ InChIKey, fallback CID; при конфликте с ChEMBL: ChEMBL первичен для bioactivity, PubChem для identifiers | unit: parsers JSON/CSV/SD; integ: CID→InChIKey, golden (compounds.ndjson) | |
| ChEMBL | `src/bioetl/sources/chembl/...` | `from bioetl.sources.chembl import ChEMBLPipeline` | `api_base, page_size, retries, backoff, activity_filters, targets_filters` | ключи assay_id/activity_id; конфликт с GtoP: приоритет ChEMBL | unit: paginator page; integ: activity_fact schema, golden (activities.parquet) | изменение схем API |
| UniProt | `src/bioetl/sources/uniprot/...` | `from bioetl.sources.uniprot import UniProtPipeline` | `api_base, fields, batch_size, retries` | ключ uniprot_id; merge с ChEMBL targets по uniprot_id | unit: TSV/JSON parser; integ: mapping, golden (targets.tsv) | колебания набора полей |
| IUPHAR_GtoP | `src/bioetl/sources/iuphar/...` | `from bioetl.sources.iuphar import IUPHARPipeline` | `api_base, endpoints, retries, backoff` | ключи target_id/ligand_id; приоритеты: UniProt по белку, IUPHAR по классификации | unit: endpoint coverage; integ: class sync, golden (iuphar_targets.jsonl) | rate-limit |
| PubMed E-utils | `src/bioetl/sources/pubmed/...` | `from bioetl.sources.pubmed import PubMedPipeline` | `api_base, api_key, term, retmax, retmode, retries, backoff` | ключ PMID; merge c Crossref/OpenAlex по DOI/PMID | unit: esearch/efetch parsers; integ: DOI↔PMID reconciliation | капризные форматы |
| OpenAlex | `src/bioetl/sources/openalex/...` | `from bioetl.sources.openalex import OpenAlexPipeline` | `api_base, mailto, per_page, retries` | ключ OpenAlexID; Crossref первичен по DOI; OpenAlex дополняет цитирования | unit: pagination; integ: works join, golden (works.jsonl) | изменения полей |
| Semantic Scholar | `src/bioetl/sources/semanticscholar/...` | `from bioetl.sources.semanticscholar import SemanticScholarPipeline` | `api_base, fields, per_page, retries` | ключ PaperId; merge с DOI-метаданными Crossref | unit: parser; integ: DOI↔PaperId consistency | лимиты API |

Ссылки на код/тесты каждого источника обязаны указываться в документации к источнику:
[ref: repo:src/bioetl/sources/<source>/@test_refactoring_32],
[ref: repo:tests/bioetl/sources/<source>/@test_refactoring_32].

# 6. Требования к слоям (@test_refactoring_32)

## 6.1 client.py (@test_refactoring_32)

**MUST**: поддерживать ретраи/экспоненциальный бэкофф; уважать Retry-After для статусов 429/503; обеспечивать rate-limit (token bucket или фиксированный RPS).
[datatracker.ietf.org @test_refactoring_32](https://datatracker.ietf.org/doc/html/rfc7231#section-7.1.3)

**SHOULD**: телеметрия запросов с корреляцией по run_id.

## 6.2 request.py (@test_refactoring_32)

**MUST**: декларативная сборка путей/параметров; унифицированные правила auth/headers.

**SHOULD**: шаблоны эндпоинтов и версионирование.

## 6.3 paginator.py (@test_refactoring_32)

**MUST**: стратегии cursor|page|offset|token|datewindow с единым интерфейсом.

**SHOULD**: восстановление по чекпоинту после сбоев.

## 6.4 parser.py (@test_refactoring_32)

**MUST**: корректный разбор JSON/CSV/XML/NDJSON; потоковая обработка крупных ответов.

**SHOULD**: унифицированные ошибки парсинга.

## 6.5 normalizer.py (@test_refactoring_32)

**MUST**: нормализация идентификаторов, единиц, онтологий; детерминированная сортировка и дедупликация.

**SHOULD**: отчёт о преобразованиях.

## 6.6 schema.py (@test_refactoring_32)

**MUST**: Pandera-схемы входа/выхода с жёсткими типами и доменами значений.
[pandera.readthedocs.io @test_refactoring_32](https://pandera.readthedocs.io/)

**SHOULD**: версионирование схем и «заморозка» набора колонок.

## 6.7 merge.py (@test_refactoring_32)

**MUST**: явные ключи слияния, приоритеты источников, политика разрешения конфликтов; фиксация обоснований решений.

**SHOULD**: трассировка lineage для конфликтов.

## 6.8 pipeline.py (@test_refactoring_32)

**MUST**: реализовать хуки extract → normalize → validate → write → run из PipelineBase.

**SHOULD**: --dry-run, лимиты и сэмплинг для отладки.

## 6.9 config.py (@test_refactoring_32)

**MUST**: Pydantic-совместимые модели; автогенерация и публикация JSON Schema конфигов; валидация при старте.
[docs.pydantic.dev @test_refactoring_32](https://docs.pydantic.dev/)

**MUST NOT**: расходящиеся имена ключей при одинаковой семантике между источниками.

## 6.10 output_writer (@test_refactoring_32)

**MUST**: атомарная запись через temp-файл и `os.replace`; фикс-сортировка и формат.
[Python documentation @test_refactoring_32](https://docs.python.org/3/library/os.html#os.replace)

# 7. Правила мерджа (MergePolicy) (@test_refactoring_32)

**Ключи идентичности**:

- Crossref: DOI
- PubChem: InChIKey (fallback CID)
- ChEMBL: assay_id, activity_id
- UniProt: uniprot_id
- PubMed: PMID
- OpenAlex: OpenAlexID
- Semantic Scholar: PaperId

**Приоритеты источников**:

- Метаданные публикаций: Crossref первичен по полям DOI/библиографии; OpenAlex/SS дополняют цитирования.
- Bioactivity/assays: ChEMBL первичен.
- Идентификаторы соединений: PubChem первичен для синонимов/CID↔InChIKey.

**Детальные политики слияния по сущностям:**

**Документы (`documents`)**:
- `doi`, `title`, `container_title`/`journal`, `published_(print|online)_date`: Crossref > PubMed > OpenAlex > ChEMBL
- `authors`: PubMed > Crossref; дедупликация по (`surname`, `initials`); порядок как в источнике с наибольшей полнотой
- `year`: из приоритетной даты публикации; при расхождении берётся год от источника, предоставившего `doi`
- Политика отказа: источник понижается, если `doi` некорректен или отсутствует минимальный набор (`title` и `date|year`)

**Таргеты (`targets`)**:
- Номенклатура: `name`, `gene_symbol`, `organism` из UniProt; при отсутствии — из ChEMBL
- Классификация/семейство: IUPHAR > ChEMBL
- Отказ: если `uniprot_accession` некорректен, унификация сводится к ChEMBL, поле помечается как «требует уточнения»

**Ассайы (`assays`)**:
- `assay_type`/`category`/`format`: соответствие BAO; при конфликте BAO-карта перекрывает `assay_type` из сырых данных ChEMBL
- Привязки к `document_chembl_id` и `target_chembl_id` обязательны, если указаны в ChEMBL

**Тест-айтемы (`testitems`)**:
- Имена/синонимы: PubChem > ChEMBL
- `salt` и `parent`-связи: пересечение карт соответствий; при конфликте отбрасывается источник без валидационного признака

**Активности (`activities`)**:
- `standard_type`, `standard_units`, `standard_value`: выбирается запись с корректной единицей, требующей минимального преобразования к целевым; если есть `pchembl_value` и валидные исходные параметры, он сохраняется
- При равной уверенности tie-breaker по полноте обязательных полей и свежести релиза источника

**Конфликты**:

**MUST**: конфликты полей фиксируются в отчёте lineage; решения детерминированы и повторяемы.

**SHOULD**: несогласованные значения выносятся в «расхождения» с указанием источника-победителя.

📄 **Полное описание**: [docs/requirements/99-data-sources-and-data-spec.md @test_refactoring_32](../docs/requirements/99-data-sources-and-data-spec.md)

# 8. Конфигурации (@test_refactoring_32)

**Расположение**: `src/bioetl/configs/pipelines/<source>.yaml`.

**Модель**: `sources/<source>/config.py`.

**Требования**:

- **MUST**: строгая валидация Pydantic-совместимыми моделями, генерация JSON Schema и проверка при запуске.
 [docs.pydantic.dev @test_refactoring_32](https://docs.pydantic.dev/)
- **MUST**: единые имена ключей для одинаковых концептов (rate_limit_rps, retries, backoff и т.п.).
- **MUST NOT**: нераспознанные ключи.

# 9. Тестирование (@test_refactoring_32)

- **Unit-тесты** (SHOULD): на каждый слой (client|paginator|parser|normalizer|schema|merge).
- **Integration-тесты** (SHOULD): на целый пайплайн; smoke-прогон и проверка golden-артефактов.
- **Контрактные тесты** (MUST):
 - валидация данных Pandera-схемами;
 - валидация конфигов Pydantic-моделями и соответствующих JSON Schema.
 [pandera.readthedocs.io @test_refactoring_32](https://pandera.readthedocs.io/)
- **Детерминизм** (MUST): повторные прогоны сравниваются с golden-файлами побайтно.

# 10. Метрики и контроль регресса (@test_refactoring_32)

Для каждого источника фиксируются в `docs/requirements/PIPELINES.md`:

- `files_before` → `files_after`, `loc_before` → `loc_after`, `public_symbols_before` → `after`;
- время прогона тестов; доля повторяющегося кода; объём выходных данных.

**Порог**: сокращение числа файлов в затронутых «семьях» не менее 30% без потери функционала; покрытие тестами не ниже базовой линии ветки.

# 11. Шаблоны разделов (@test_refactoring_32)


## 11.1 Карта источников (@test_refactoring_32)

```markdown
## Источники (@test_refactoring_32)
| source | owner | data domain | endpoints | rate limits | auth | notes |
|--------|-------|-------------|-----------|-------------|------|-------|
| Crossref | publications | metadata | /works, /funders | RPS=N | none | ... |
...
```


## 11.2 Спецификация пайплайна <source> (@test_refactoring_32)

```markdown
### <source> (@test_refactoring_32)
Layout: src/bioetl/sources/<source>/{client,request,paginator,parser,normalizer,schema,merge,pipeline,config}.py
Public API: from bioetl.sources.<source> import <Source>Pipeline
Config (MUST): <перечень ключей + типы>
MergePolicy (MUST): <ключи, приоритеты, правила конфликтов>
Tests: <unit по слоям, integ + golden>
Risks: <лимиты, нестабильные поля, схемные расхождения>
Links: [ref: repo:src/bioetl/sources/<source>/@test_refactoring_32],
    [ref: repo:tests/bioetl/sources/<source>/@test_refactoring_32]
```


## 11.3 Единые правила именования и раскладки (@test_refactoring_32)

- Файлы слоёв: строго как в шаблоне (§3).
- Публичный API: реэкспорт пайплайна только из `__init__.py` источника.
- Общие абстракции: размещаются в [ref: repo:src/bioetl/core/@test_refactoring_32].
- Запрещены циклические импорты и зависимость одного источника от другого (общие части выносить в `core/`).

# 12. Acceptance Criteria (@test_refactoring_32)

- **MUST**: для каждого источника создан единый пайплайн по шаблону (§3) и контракту (§4).
- **MUST**: конфиги валидируются Pydantic-моделями; JSON Schema опубликованы.
 [docs.pydantic.dev @test_refactoring_32](https://docs.pydantic.dev/)
- **MUST**: данные валидируются Pandera-схемами.
 [pandera.readthedocs.io @test_refactoring_32](https://pandera.readthedocs.io/)
- **MUST**: вывод детерминирован; запись атомарна.
 [Python documentation @test_refactoring_32](https://docs.python.org/3/library/os.html#os.replace)
- **MUST**: нет дублирующих реализаций слоёв; различия выражены стратегиями/параметрами.
- **SHOULD**: модульные и интеграционные тесты зелёные; golden-артефакты стабильны.
- **SHOULD**: достигнуты целевые метрики сокращения дубликатов (§10).

# 13. Архитектурные компоненты (@test_refactoring_32)

Все пайплайны используют унифицированные компоненты из `src/bioetl/core/`:


## 13.1 UnifiedLogger (@test_refactoring_32)

**Назначение**: структурированное, безопасное, воспроизводимое логирование.

**Компоненты**:
- Структурированный вывод через structlog
- UTC timestamps для детерминизма
- Контекстные переменные (run_id, stage, trace_id)
- Автоматическое редактирование секретов
- OpenTelemetry интеграция (опционально)

**Обязательные поля контекста**:
- `run_id`, `stage`, `actor`, `source`, `generated_at` — всегда
- `endpoint`, `attempt`, `duration_ms`, `params` — для HTTP-запросов

**Режимы**:
- development: text, DEBUG, telemetry off
- production: JSON, INFO, telemetry on, rotation
- testing: text, WARNING, telemetry off

📄 **Полное описание**: [docs/requirements/01-logging-system.md @test_refactoring_32](../docs/requirements/01-logging-system.md)


## 13.2 UnifiedOutputWriter (@test_refactoring_32)

**Назначение**: детерминированный вывод данных с качественными метриками.

**Компоненты**:
- Атомарная запись через run-scoped временные директории с `os.replace`
- Поддержка CSV и Parquet форматов
- Автоматическая генерация QC отчетов
- Опциональные correlation отчеты (по умолчанию выключены)
- Валидация через Pandera схемы
- Run manifests для отслеживания
- Каноническая сериализация для воспроизводимых хешей

**Режимы**:
- **Standard**: `dataset.csv`, `quality_report.csv`
- **Extended**: добавляет `meta.yaml`, `run_manifest.json`

**Инварианты детерминизма**:
- Checksums стабильны при одинаковом вводе (SHA256)
- Порядок строк фиксирован (deterministic sort)
- Column order **только** из Schema Registry
- NA-policy: `""` для строк, `null` для чисел
- Каноническая сериализация (JSON+ISO8601, float=%.6f)

📄 **Полное описание**: [docs/requirements/02-io-system.md @test_refactoring_32](../docs/requirements/02-io-system.md)


## 13.3 UnifiedAPIClient (@test_refactoring_32)

**Назначение**: надежный, масштабируемый доступ к внешним API.

**Компоненты**:
- Опциональный TTL-кэш
- Circuit breaker для защиты от каскадных ошибок
- Fallback manager со стратегиями отката
- Token bucket rate limiter с jitter
- Exponential backoff с giveup условиями
- Унифицированные стратегии пагинации

**Политика ретраев**:
- 2xx, 3xx: успех
- 429: respect Retry-After, ретраить
- 4xx (кроме 429): fail-fast
- 5xx: exponential backoff, retry

📄 **Полное описание**: [docs/requirements/03-data-extraction.md @test_refactoring_32](../docs/requirements/03-data-extraction.md)


## 13.4 UnifiedSchema (@test_refactoring_32)

**Назначение**: строгая валидация и стандартизация данных.

**Компоненты**:
- Модульная система нормализаторов (реестр)
- Источник-специфичные схемы для Document, Target, TestItem
- Pandera валидация с метаданными нормализации
- Фабрики полей для типовых идентификаторов
- Автоматические QC проверки

**Категории нормализаторов**:
- String, Numeric, DateTime, Boolean
- Chemistry (SMILES, InChI)
- Identifier (DOI, PMID, ChEMBL ID, UniProt, PubChem CID)
- Ontology (MeSH, GO terms)

📄 **Полное описание**: [docs/requirements/04-normalization-validation.md @test_refactoring_32](../docs/requirements/04-normalization-validation.md)

# 14. Ссылки на детальные спецификации (@test_refactoring_32)

Все компоненты имеют детальные спецификации в `docs/requirements/`:

| Документ | Описание |
|----------|----------|
| [00-architecture-overview.md @test_refactoring_32](../docs/requirements/00-architecture-overview.md) | Архитектурные принципы и обзор компонентов |
| [01-logging-system.md @test_refactoring_32](../docs/requirements/01-logging-system.md) | UnifiedLogger: структура, контекст, режимы |
| [02-io-system.md @test_refactoring_32](../docs/requirements/02-io-system.md) | UnifiedOutputWriter: атомарная запись, QC, метаданные |
| [03-data-extraction.md @test_refactoring_32](../docs/requirements/03-data-extraction.md) | UnifiedAPIClient: отказоустойчивость, пагинация |
| [04-normalization-validation.md @test_refactoring_32](../docs/requirements/04-normalization-validation.md) | UnifiedSchema: нормализаторы, Pandera схемы |
| [05-assay-extraction.md @test_refactoring_32](../docs/requirements/05-assay-extraction.md) | Assay pipeline спецификация |
| [06-activity-data-extraction.md @test_refactoring_32](../docs/requirements/06-activity-data-extraction.md) | Activity pipeline спецификация |
| [07a-testitem-extraction.md @test_refactoring_32](../docs/requirements/07a-testitem-extraction.md) | Testitem extraction спецификация |
| [07b-testitem-data-extraction.md @test_refactoring_32](../docs/requirements/07b-testitem-data-extraction.md) | Testitem data extraction спецификация |
| [08-target-data-extraction.md @test_refactoring_32](../docs/requirements/08-target-data-extraction.md) | Target pipeline спецификация |
| [09-document-chembl-extraction.md @test_refactoring_32](../docs/requirements/09-document-chembl-extraction.md) | Document pipeline спецификация |
| [10-configuration.md @test_refactoring_32](../docs/requirements/10-configuration.md) | Конфигурация: YAML, Pydantic, CLI |
| [99-data-sources-and-data-spec.md @test_refactoring_32](../docs/requirements/99-data-sources-and-data-spec.md) | Спецификация источников данных и требований |
| [99-final-tech-spec.md @test_refactoring_32](../docs/requirements/99-final-tech-spec.md) | Итоговая техническая спецификация |
