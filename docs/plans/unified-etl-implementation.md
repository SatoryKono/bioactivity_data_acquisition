# План реализации унифицированной ETL-архитектуры

## Общая стратегия

Проект начинается с нуля. Порядок реализации: инфраструктура → базовые компоненты → пайплайны по порядку документов (Assay → Activity → Testitem → Target → Document). Каждый этап завершается промежуточной проверкой.

---

## Этап 1: Скелет проекта и зависимости

### 1.1 Создать структуру каталогов

```
src/bioetl/{__init__.py, core/, config/, normalizers/, schemas/, pipelines/, cli/}
configs/{base.yaml, profiles/, pipelines/}
tests/{unit/, integration/, golden/, fixtures/}
```

### 1.2 Создать pyproject.toml

- Зависимости: pandas, pandera, requests, backoff, structlog, typer, pydantic, pyyaml, tenacity
- Dev: pytest, pytest-cov, mypy, ruff, pre-commit

### 1.3 Настроить CI/CD

- `.pre-commit-config.yaml`: ruff (lint+format), mypy, trailing-whitespace
- `.github/workflows/ci.yml`: lint → test → coverage
- `.gitignore`: `__pycache__/`, `.mypy_cache/`, `data/cache/`, `*.pyc`, `.env`

### 1.4 Промежуточная проверка

```bash
# Проверка структуры
ls -R src/ configs/ tests/
# Установка зависимостей
pip install -e ".[dev]"
# Pre-commit hooks
pre-commit install && pre-commit run --all-files
```

---

## Этап 2: Система конфигурации (PipelineConfig)

### 2.1 Создать Pydantic модели

**Файл:** `src/bioetl/config/models.py`

- `PipelineConfig`, `HttpConfig`, `CacheConfig`, `PathConfig`, `DeterminismConfig`, `QCConfig`, `PostprocessConfig`
- Computed field `config_hash` (SHA256 canonical JSON без paths/secrets)

### 2.2 Реализовать YAML загрузчик

**Файл:** `src/bioetl/config/loader.py`

- `load_config(path, overrides)`: base.yaml → extends рекурсивно → ENV (BIOETL_*) → CLI overrides
- Валидация через Pydantic

### 2.3 Создать базовые конфиги

- `configs/base.yaml`: глобальные настройки (http timeouts, cache, paths)
- `configs/profiles/dev.yaml`: extends base, retries=2, cache=true
- `configs/profiles/prod.yaml`: extends base, retries=5, cache=true, strict=true
- `configs/profiles/test.yaml`: extends base, cache=false, limit=10

### 2.4 Unit-тесты конфигурации

**Файл:** `tests/unit/test_config_loader.py`

- Тест наследования (extends)
- Тест приоритетов (CLI > ENV > profile > base)
- Тест `config_hash` стабильности
- Тест невалидных значений (должен упасть)

### 2.5 Промежуточная проверка

```bash
# Загрузка конфига
python -c "from bioetl.config import load_config; print(load_config('configs/profiles/dev.yaml', {}))"
# Тесты
pytest tests/unit/test_config_loader.py -v
```

---

## Этап 3: UnifiedLogger

### 3.1 Реализовать UnifiedLogger

**Файл:** `src/bioetl/core/logger.py`

- `UnifiedLogger.setup(mode, run_id)`: настройка structlog с processors
- `SecurityProcessor`: редакция secrets (api_key, token, password) через regex
- `TimeStamper(fmt="iso", utc=True)`: UTC timestamps
- ContextVar для `run_id`, `stage`, `entity`
- 3 режима: development (DEBUG, readable), production (INFO, JSON), testing (WARNING, minimal)

### 3.2 Unit-тесты логирования

**Файл:** `tests/unit/test_logger.py`

- Тест редакции секретов: `api_key=abc123` → `api_key=***REDACTED***`
- Тест UTC timestamps (формат ISO8601)
- Тест ContextVar propagation (`run_id` присутствует в логах)
- Тест режимов (dev/prod/test)

### 3.3 Промежуточная проверка

```bash
# Пример использования
python -c "
from bioetl.core.logger import UnifiedLogger
UnifiedLogger.setup('development', run_id='test-123')
log = UnifiedLogger.get('test')
log.info('test_event', api_key='secret123')
"
# Должно показать: api_key=***REDACTED***, run_id=test-123
pytest tests/unit/test_logger.py -v
```

---

## Этап 4: UnifiedAPIClient

### 4.1 Реализовать базовые компоненты

**Файл:** `src/bioetl/core/api_client.py`

- `CircuitBreaker`: 5 failures → open (60s) → half-open → closed
- `TokenBucketLimiter`: QPS с jitter (±20%)
- `TTLCache`: persistent кэш с release-scoped ключами

### 4.2 Реализовать UnifiedAPIClient

- `request_json(url, params, method)`:
  - Circuit breaker check
  - Rate limit wait
  - Cache lookup (GET only)
  - HTTP request с timeout
  - Retry-After protocol (HTTP 429)
  - Fail-fast на 4xx (кроме 429)
  - Exponential backoff на 5xx/timeout
- `request_batch(ids, endpoint, batch_size)`:
  - Split на батчи (≤25)
  - Adaptive retry: timeout → рекурсивное разбиение
  - POST override если URL >2000 chars
- `paginate(url, strategy)`: offset/cursor/batch_ids strategies

### 4.3 Интеграционные тесты API

**Файл:** `tests/integration/test_api_client.py`

- Mock HTTP server (pytest-httpserver)
- Тест retry на 5xx (должен повторить 3 раза)
- Тест Retry-After (должен подождать указанное время)
- Тест circuit breaker (5 failures → открыт на 60s)
- Тест batch splitting (timeout → разбиение пополам)
- Тест pagination (offset/cursor)

### 4.4 Промежуточная проверка

```bash
# Integration тесты с mock сервером
pytest tests/integration/test_api_client.py -v --log-cli-level=DEBUG
# Проверка cache
python -c "
from bioetl.core.api_client import UnifiedAPIClient
client = UnifiedAPIClient(config, cache_dir='data/cache/test')
resp = client.request_json('https://httpbin.org/get')
print(resp)
"
```

---

## Этап 5: UnifiedSchema и нормализаторы

### 5.1 Реализовать базовые нормализаторы

**Файлы:**

- `src/bioetl/normalizers/base.py`: `BaseNormalizer` (ABC)
- `src/bioetl/normalizers/string.py`: `StringNormalizer` (strip, lowercase, max_length)
- `src/bioetl/normalizers/numeric.py`: `NumericNormalizer` (type coercion, precision)
- `src/bioetl/normalizers/chemistry.py`: `SMILESNormalizer`, `InChINormalizer`
- `src/bioetl/normalizers/identifier.py`: `DOINormalizer`, `ChEMBLIDNormalizer`, `UniProtNormalizer`

### 5.2 Создать NormalizerRegistry

**Файл:** `src/bioetl/normalizers/registry.py`

- Регистрация нормализаторов по типу
- `NormalizerRegistry.get(type) → normalizer`

### 5.3 Реализовать Schema Registry

**Файл:** `src/bioetl/schemas/registry.py`

- `SchemaRegistry.register(entity, version, schema)`
- `SchemaRegistry.get(entity, version='latest')`
- `_validate_compatibility()`: semantic versioning (major change → fail-fast)

### 5.4 Создать Pandera схемы

**Файлы:** `src/bioetl/schemas/{input,raw,output}_schemas.py`

- `AssaySchema`, `ActivitySchema`, `TestitemSchema`, `TargetSchema`, `DocumentSchema`
- Strict mode, coerce types, column order из `column_order` config
- NA-policy: строки→"", числа→pd.NA
- Precision-policy: %.6f для `standard_value`, %.2f для `molecular_weight`

### 5.5 Unit-тесты нормализаторов и схем

**Файл:** `tests/unit/test_normalizers.py`

- Тест DOI: `https://doi.org/10.1234/test` → `10.1234/test`
- Тест SMILES canonicalization (если RDKit доступен)
- Тест StringNormalizer: `  Test  ` → `test`

**Файл:** `tests/unit/test_schemas.py`

- Тест валидации ActivitySchema (валидные данные → pass)
- Тест schema drift detection (major version → fail)
- Тест column order enforcement

### 5.6 Golden fixtures

**Файл:** `tests/golden/normalizers_golden.yaml`

- Фикстуры для воспроизводимости нормализации

### 5.7 Промежуточная проверка

```bash
pytest tests/unit/test_normalizers.py tests/unit/test_schemas.py -v
# Проверка drift detection
python -c "
from bioetl.schemas.registry import SchemaRegistry
from bioetl.schemas.output_schemas import ActivitySchema
SchemaRegistry.register('activity', '1.0.0', ActivitySchema)
SchemaRegistry.register('activity', '2.0.0', ActivitySchema)  # Должен fail
"
```

---

## Этап 6: UnifiedOutputWriter и QC

### 6.1 Реализовать атомарную запись

**Файл:** `src/bioetl/core/output_writer.py`

- `AtomicWriter.write(df, path)`: запись в `.tmp` → `os.replace()` → атомарно
- Поддержка CSV и Parquet

### 6.2 Реализовать QC генераторы

- `QualityReportGenerator`: completeness, uniqueness, range checks, pattern validation
- `CorrelationReportGenerator` (extended mode): Pearson, Spearman, Cramér's V, point-biserial, eta-squared

### 6.3 Реализовать UnifiedOutputWriter

- `write(df, output_path, metadata)`:
  1. Pandera validation (lazy)
  2. Сортировка по business keys
  3. Column order из schema
  4. Canonical serialization (NA policy, precision)
  5. Запись через AtomicWriter
  6. QC report generation
  7. Correlation report (если `--extended`)
  8. meta.yaml с checksums, row_count, config

### 6.4 Реализовать ManifestWriter

**Файл:** `src/bioetl/core/manifest_writer.py`

- `write_manifest(run_id, artifacts)`: манифест всех артефактов прогона

### 6.5 Unit-тесты output writer

**Файл:** `tests/unit/test_output_writer.py`

- Тест атомарной записи (прерывание → `.tmp` не влияет на старый файл)
- Тест canonical serialization (NA→"", precision)
- Тест column order enforcement
- Тест checksum стабильности (одинаковые данные → одинаковый checksum)

### 6.6 Golden run тесты

**Файл:** `tests/golden/test_output_determinism.py`

- Прогон с фикстурой → сравнение checksum с golden

### 6.7 Промежуточная проверка

```bash
pytest tests/unit/test_output_writer.py -v
pytest tests/golden/test_output_determinism.py -v
# Проверка артефактов
python -c "
from bioetl.core.output_writer import UnifiedOutputWriter
writer = UnifiedOutputWriter(config, schema)
writer.write(df, 'data/output/test/test.csv', metadata)
ls -la data/output/test/
# Должны быть: test.csv, test_qc.csv, test_meta.yaml
"
```

---

## Этап 7: CLI и оркестратор пайплайна

### 7.1 Реализовать базовый класс пайплайна

**Файл:** `src/bioetl/pipelines/base.py`

- `PipelineBase` (ABC): `extract()`, `transform()`, `validate()`, `export()`
- `run()`: orchestration с логированием стадий

### 7.2 Реализовать CLI

**Файл:** `src/bioetl/cli/main.py` (typer)

- `bioetl pipeline run <name>`: запуск пайплайна
- Флаги: `--config`, `--set key=value`, `--golden`, `--fail-on-schema-drift`, `--extended`, `--mode`, `--sample N`, `--dry-run`, `--verbose`
- `bioetl pipeline list`: список доступных пайплайнов
- `bioetl pipeline validate <name>`: dry-run валидация

### 7.3 Интеграционные тесты CLI

**Файл:** `tests/integration/test_cli.py`

- Тест `bioetl pipeline list` (должен вернуть список)
- Тест `--dry-run` (не должен создать артефакты)
- Тест `--sample 10` (должен ограничить до 10 строк)
- Тест `--fail-on-schema-drift` (major version change → exit 1)

### 7.4 Промежуточная проверка

```bash
bioetl --help
bioetl pipeline list
bioetl pipeline validate assay --config configs/profiles/test.yaml
```

---

## Этап 8: Пайплайн Assay

### 8.1 Реализовать Assay Pipeline

**Файл:** `src/bioetl/pipelines/assay.py`

- `extract()`: ChEMBL API `/assay?limit=1000`, batch≤25, pagination offset
- `transform()`:
  - Long-format explode (assay_parameters, target_chembl_id)
  - Whitelist enrichment (assay_type, relationship_type)
  - Flatten nested JSON
- `validate()`: `AssaySchema` validation
- `export()`: сортировка по `assay_chembl_id`, QC reports

### 8.2 Создать AssaySchema

**Файл:** `src/bioetl/schemas/output_schemas.py`

- Колонки: assay_chembl_id, assay_type, description, target_chembl_id, relationship_type, confidence_score, ...
- Constraints: assay_chembl_id (unique, regex), confidence_score (0-9)

### 8.3 Конфигурация Assay

**Файл:** `configs/pipelines/assay.yaml`

- `extends: ../base.yaml`
- `pipeline.name: assay`, `pipeline.batch_size: 25`
- `postprocess.enrichment.whitelist: {assay_type: [...], relationship_type: [...]}`

### 8.4 Unit-тесты Assay

**Файл:** `tests/unit/test_assay_pipeline.py`

- Тест long-format explode (1 assay + 3 targets → 3 rows)
- Тест whitelist enrichment (unknown assay_type → warning)
- Тест batch splitting (50 IDs → 2 batches по 25)

### 8.5 Интеграционный тест

**Файл:** `tests/integration/test_assay_pipeline.py`

- End-to-end с mock ChEMBL API
- Проверка артефактов: assay.csv, assay_qc.csv, assay_meta.yaml

### 8.6 Промежуточная проверка

```bash
bioetl pipeline run assay --config configs/profiles/test.yaml --sample 100 --verbose
# Проверка выходных данных
cat data/output/assay/assay_*_meta.yaml
pytest tests/unit/test_assay_pipeline.py tests/integration/test_assay_pipeline.py -v
```

---

## Этап 9: Пайплайн Activity

### 9.1 Реализовать Activity Pipeline

**Файл:** `src/bioetl/pipelines/activity.py`

- `extract()`:
  - Batch IDs strategy из `/activity?assay_chembl_id=...`
  - Adaptive retry: timeout → split batch пополам
  - Partial failure handling (requeue)
- `transform()`:
  - Нормализация metrics: standard_type, standard_value, standard_units
  - BAO term mapping (assay_type → bao_endpoint)
  - Flatten relations (target_chembl_id из assay)
- `validate()`: `ActivitySchema` validation
- `export()`: сортировка по `activity_id`, QC с порогом пропусков (>20% → warning)

### 9.2 Создать ActivitySchema

**Файл:** `src/bioetl/schemas/output_schemas.py`

- Колонки: activity_id, assay_chembl_id, molecule_chembl_id, standard_type, standard_value, standard_units, pchembl_value, ...
- Constraints: activity_id (unique), standard_value (≥0), pchembl_value (0-14)

### 9.3 Конфигурация Activity

**Файл:** `configs/pipelines/activity.yaml`

- `pipeline.batch_size: 25`, `pipeline.partial_failure_threshold: 0.1`
- `qc.thresholds.missing_standard_value: 0.2`

### 9.4 Unit-тесты Activity

**Файл:** `tests/unit/test_activity_pipeline.py`

- Тест batch IDs strategy
- Тест нормализация units (µM → uM)
- Тест BAO term mapping
- Тест QC threshold (>20% missing → warning)

### 9.5 Интеграционный тест с Retry-After

**Файл:** `tests/integration/test_activity_retry.py`

- Mock API с HTTP 429 + Retry-After: 5
- Проверка: должен подождать 5s и повторить

### 9.6 Промежуточная проверка

```bash
bioetl pipeline run activity --config configs/profiles/test.yaml --sample 100
pytest tests/unit/test_activity_pipeline.py tests/integration/test_activity_retry.py -v
```

---

## Этап 10: Пайплайн Testitem

### 10.1 Реализовать Testitem Pipeline

**Файл:** `src/bioetl/pipelines/testitem.py`

- `extract()`:
  - ChEMBL molecules API batch≤100
  - PubChem enrichment (CID → synonyms, properties) с persistent cache
  - Graceful degradation: PubChem failure → ChEMBL only
- `transform()`:
  - Flatten hierarchy (molecule_chembl_id → structures → synonyms)
  - Chemistry normalization (SMILES, InChI canonical)
  - Property calculations (если RDKit)
- `validate()`: `TestitemSchema`
- `export()`: сортировка по `molecule_chembl_id`, correlation report (extended)

### 10.2 Создать TestitemSchema

**Файл:** `src/bioetl/schemas/output_schemas.py`

- Колонки: molecule_chembl_id, canonical_smiles, inchi_key, molecular_weight, alogp, pubchem_cid, synonyms, ...
- Constraints: molecule_chembl_id (unique), molecular_weight (>0)

### 10.3 Конфигурация Testitem

**Файл:** `configs/pipelines/testitem.yaml`

- `pipeline.enrichment_sources: [chembl, pubchem]`
- `cache.pubchem.ttl: 2592000` (30 дней)

### 10.4 Unit-тесты Testitem

**Файл:** `tests/unit/test_testitem_pipeline.py`

- Тест flatten synonyms (list → comma-separated string)
- Тест SMILES canonicalization
- Тест PubChem fallback (API down → skip enrichment)

### 10.5 Интеграционный тест

**Файл:** `tests/integration/test_testitem_pubchem.py`

- Mock PubChem API
- Проверка cache hit/miss

### 10.6 Промежуточная проверка

```bash
bioetl pipeline run testitem --config configs/profiles/test.yaml --sample 50 --extended
# Проверка correlation report
ls data/output/testitem/*correlation_report*/
pytest tests/unit/test_testitem_pipeline.py tests/integration/test_testitem_pubchem.py -v
```

---

## Этап 11: Пайплайн Target

### 11.1 Реализовать Target Pipeline (multi-stage)

**Файл:** `src/bioetl/pipelines/target.py`

- `extract_chembl()`: `/target`, `/target_component`, `/protein_classification`
- `extract_uniprot()`: UniProt REST API (accession → gene, organism, function)
- `extract_iuphar()`: IUPHAR/BPS API (target → ligands, family)
- `extract_orthologs()`: ortholog/isoform mapping
- `transform()`:
  - Join ChEMBL + UniProt + IUPHAR по accession
  - Materialize 4 таблицы: targets, target_components, protein_class, xref
  - Cross-reference normalization (DOI, PubMed ID)
- `validate()`: 4 schemas
- `export()`: 4 файла с отдельными QC reports

### 11.2 Создать Target Schemas

**Файлы:** `src/bioetl/schemas/target_schemas.py`

- `TargetSchema`: target_chembl_id, pref_name, target_type, organism, ...
- `TargetComponentSchema`: component_id, accession, component_type, ...
- `ProteinClassSchema`: protein_class_id, class_level, ...
- `XrefSchema`: xref_id, source, source_id, target_chembl_id, ...

### 11.3 Конфигурация Target

**Файл:** `configs/pipelines/target.yaml`

- `pipeline.stages: [chembl, uniprot, iuphar, orthologs]`
- `http.uniprot.rate_limit: 5`, `http.iuphar.rate_limit: 2`

### 11.4 Unit-тесты Target

**Файл:** `tests/unit/test_target_pipeline.py`

- Тест join ChEMBL + UniProt (matching accession)
- Тест ortholog mapping
- Тест materialization 4 таблиц

### 11.5 Интеграционные тесты

**Файл:** `tests/integration/test_target_multi_source.py`

- Mock ChEMBL, UniProt, IUPHAR APIs
- Проверка 4 output файлов

### 11.6 Промежуточная проверка

```bash
bioetl pipeline run target --config configs/profiles/test.yaml --sample 20
ls data/output/target/
# Должны быть: targets.csv, target_components.csv, protein_class.csv, xref.csv + meta/qc для каждого
pytest tests/unit/test_target_pipeline.py tests/integration/test_target_multi_source.py -v
```

---

## Этап 12: Пайплайн Document

### 12.1 Реализовать Document Pipeline

**Файл:** `src/bioetl/pipelines/document.py`

- `extract()`:
  - Режим `chembl`: только ChEMBL `/document`
  - Режим `all`: ChEMBL + external (PubMed, Crossref, OpenAlex, Semantic Scholar)
- Адаптеры для внешних источников:
  - `PubMedAdapter`: DOI/PMID → abstract, authors, journal
  - `CrossrefAdapter`: DOI → citation metadata
  - `OpenAlexAdapter`: DOI → citation count, concepts
  - `SemanticScholarAdapter`: DOI → citations, references
- `transform()`:
  - Priority merge (ChEMBL > PubMed > Crossref > OpenAlex > Semantic Scholar)
  - Identifier normalization (DOI, PMID, PMCID)
  - Author list flatten
- `validate()`: `DocumentSchema`
- `export()`: сортировка по `document_id`, extended correlation

### 12.2 Создать DocumentSchema

**Файл:** `src/bioetl/schemas/output_schemas.py`

- Колонки: document_id, doi, pubmed_id, title, authors, journal, year, abstract, ...
- Constraints: doi (regex), pubmed_id (int), year (1900-2100)

### 12.3 Конфигурация Document

**Файл:** `configs/pipelines/document.yaml`

- `pipeline.mode: all` (или `chembl`)
- `pipeline.external_sources: [pubmed, crossref, openalex, semantic_scholar]`
- `pipeline.priority: [chembl, pubmed, crossref, openalex, semantic_scholar]`
- Rate limits для каждого API

### 12.4 Unit-тесты Document

**Файл:** `tests/unit/test_document_pipeline.py`

- Тест priority merge (ChEMBL abstract > PubMed abstract)
- Тест DOI normalization (`https://doi.org/10.1234/test` → `10.1234/test`)
- Тест author flatten (list → semicolon-separated)

### 12.5 Интеграционные тесты адаптеров

**Файлы:** `tests/integration/test_document_adapters.py`

- Mock PubMed, Crossref, OpenAlex, Semantic Scholar APIs
- Проверка fallback (PubMed fail → Crossref)

### 12.6 Промежуточная проверка

```bash
bioetl pipeline run document --config configs/profiles/test.yaml --sample 30 --set pipeline.mode=all --extended
pytest tests/unit/test_document_pipeline.py tests/integration/test_document_adapters.py -v
```

---

## Этап 13: Тестирование и финализация

### 13.1 Выполнить полный тест-план

**Файл:** `docs/test-plan.md`

- Unit tests: coverage ≥80%
- Integration tests: все пайплайны end-to-end
- Golden tests: determinism checksums
- Property-based tests (hypothesis): нормализаторы

### 13.2 Настроить golden fixtures

**Каталог:** `tests/golden/`

- Фикстуры для каждого пайплайна
- Эталонные checksums
- `test_golden_run.py`: прогон → сравнение с эталоном

### 13.3 Финализировать CI

**Файл:** `.github/workflows/ci.yml`

- Matrix: Python 3.10, 3.11, 3.12
- Steps: install → lint (ruff) → type check (mypy) → unit tests → integration tests → golden tests → coverage report
- Блокировка PR при coverage <80%

### 13.4 Обновить документацию

- `README.md`: установка, быстрый старт, примеры
- `docs/architecture.md`: диаграммы компонентов
- `docs/pipelines/`: детали каждого пайплайна
- `docs/schemas/`: Pandera schema reference
- `docs/configuration.md`: все параметры конфигурации
- `CHANGELOG.md`: initial release 1.0.0

### 13.5 Миграционные гайды

**Каталог:** `docs/migration-guides/`

- `v3.0-activity-batch-ids.md` (уже есть)
- Добавить гайды для других breaking changes (если есть)

### 13.6 Финальная проверка

```bash
# Pre-commit hooks
pre-commit run --all-files
# Полный тест-набор
pytest tests/ -v --cov=src/bioetl --cov-report=html --cov-fail-under=80
# Golden run всех пайплайнов
bioetl pipeline run assay --config configs/profiles/prod.yaml --golden
bioetl pipeline run activity --config configs/profiles/prod.yaml --golden
bioetl pipeline run testitem --config configs/profiles/prod.yaml --golden
bioetl pipeline run target --config configs/profiles/prod.yaml --golden
bioetl pipeline run document --config configs/profiles/prod.yaml --golden --set pipeline.mode=all
# Проверка checksums
python scripts/verify_golden_checksums.py
```

---

## Критерии приемки

### Инфраструктура

- ✓ Структура проекта соответствует спецификации
- ✓ Все зависимости установлены и работают
- ✓ Pre-commit hooks настроены (ruff, mypy)
- ✓ CI pipeline работает без ошибок

### Базовые компоненты

- ✓ UnifiedLogger: UTC, secret redaction, ContextVar, 3 режима
- ✓ UnifiedAPIClient: circuit breaker, rate limit, Retry-After, pagination
- ✓ UnifiedSchema: normalizers, Pandera validation, schema drift detection
- ✓ UnifiedOutputWriter: atomic writes, QC/correlation reports, checksums

### Пайплайны

- ✓ Assay: batch≤25, long-format, whitelist enrichment, QC
- ✓ Activity: batch IDs, unit normalization, BAO mapping, QC thresholds
- ✓ Testitem: ChEMBL+PubChem, chemistry normalization, correlation
- ✓ Target: 4 tables, multi-source (ChEMBL/UniProt/IUPHAR), orthologs
- ✓ Document: режимы chembl/all, 4 адаптера, priority merge

### Качество

- ✓ Unit tests coverage ≥80%
- ✓ Integration tests для всех API interactions
- ✓ Golden tests: deterministic checksums
- ✓ Документация полная и актуальная

### Детерминизм

- ✓ UTC timestamps везде
- ✓ Canonical sorting по business keys
- ✓ NA-policy: строки→"", числа→pd.NA
- ✓ Precision-policy: %.6f для standard_value
- ✓ Checksums совпадают при повторных прогонах

