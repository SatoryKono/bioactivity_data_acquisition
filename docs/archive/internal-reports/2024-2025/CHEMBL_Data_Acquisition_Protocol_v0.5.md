## DATA PROCESSING PROTOCOL

### ChEMBL BIOACTIVITY DATA COLLECTION — v0.5 (октябрь 2025)

- **Protocol Title**: ChEMBL Bioactivity Data Collection
- **Protocol Number**: CHEMBL/DPP01.5
- **Protocol Version & Date**: Version 0.5 — October 2025
- **Study Title**: A Statistical Evaluation of Experimental Uncertainty of Heterogeneous Public Bioactivity Data

- **Prepared**:
  - Amir Mrasov — Date/Signature: ______
  - Yaroslav Timofeev — Date/Signature: ______

- **Approved**:
  - Oleg Stroganov — Date/Signature: ______
  - Fedor Novikov — Date/Signature: ______

---

### Введение

Протокол приобретения данных ChEMBL стандартизует извлечение, нормализацию и контроль качества данных о биоактивности для воспроизводимой аналитики. Документ согласован с текущей реализацией проекта `bioactivity-data-acquisition` (v0.1.0) и отражает архитектуру из пяти ETL-пайплайнов: `documents`, `target`, `assay`, `testitem` (молекулы), `activity`. Протокол обеспечивает:

- единые правила доступа к REST API (ChEMBL и вторичные источники);
- детерминированное формирование артефактов (CSV/YAML, стабильная сортировка и структура);
- встроенные проверки качества (QC) и опциональный корреляционный анализ;
- воспроизводимость за счет фиксации версий, логирования и конфигураций.

Контекст: ChEMBL — признанная база данных о биоактивности. Разнородность источников и форматов требует унификации. Данный протокол синхронизирован с кодом (см. `src/library/**`, `src/scripts/**`, `configs/config_*.yaml`, `docs/pipelines/**`).

---

### 01. Программное обеспечение для обработки данных

- **Язык и версии**:
  - Минимально: Python 3.10
  - Рекомендуется: Python 3.12

- **Зависимости (основные, см. `pyproject.toml`)**:
  - HTTP и ретраи: `requests>=2.31,<3.0`, `backoff>=2.2,<3.0`
  - Обработка данных: `pandas>=2.1,<3.0`, `scipy>=1.10,<2.0`, `scikit-learn>=1.3,<2.0`
  - Конфигурации/валидация: `pydantic>=1.10,<3.0`, `pydantic-settings>=2.0,<3.0`, `pyyaml>=6.0,<7.0`, `jsonschema>=4.0,<5.0`
  - Валидация таблиц: `pandera[io]>=0.18,<1.0`
  - CLI/логирование: `typer[all]>=0.9,<1.0`, `structlog>=24.1,<25.0`, `rich>=13.0,<14.0`
  - Наблюдаемость: `opentelemetry-*` (api, sdk, requests, jaeger)
  - Безопасность: `defusedxml>=0.7,<1.0`

- **Клиенты API**: используется собственный HTTP-клиент и фабрика клиентов (`library.clients.*`), включая `ChEMBL`, `Crossref`, `OpenAlex`, `PubMed`, `Semantic Scholar`, `UniProt`, `IUPHAR`, `PubChem`.

- **Система логирования**: структурные логи через `structlog` и модуль `library.logging_setup`. Логи сохраняются в `logs/` с указанием времени, уровня, модуля и ключевых параметров.

- **Воспроизводимость**:
  - детерминистическая сериализация (`library.etl.load.write_deterministic_csv`);
  - фиксация версий источников (включая `chembl_release` из API статуса);
  - метаданные выгрузки (`*_meta.yaml`), включающие контрольные суммы файлов (SHA256), параметры извлечения и счётчики строк;
  - независимые окружения (`venv`/`conda`), зависимости — в `pyproject.toml`.

---

### 02. Источники данных

- **Первичные источники**:
  - ChEMBL REST API (`https://www.ebi.ac.uk/chembl/api/data`) — документы, мишени, ассейи, молекулы, активности; клиент `library.clients.chembl`.

- **Вторичные источники**:
  - CrossRef (`https://api.crossref.org/works`) — библиографические метаданные;
  - OpenAlex (`https://api.openalex.org/works`) — библиографические данные и связи;
  - PubMed/EuropePMC (`https://eutils.ncbi.nlm.nih.gov/entrez/eutils/`) — публикации;
  - Semantic Scholar (`https://api.semanticscholar.org/graph/v1/paper`) — метаданные публикаций;
  - UniProt (`https://rest.uniprot.org/uniprotkb`) — обогащение таргетов;
  - IUPHAR/BPS Guide to Pharmacology — классификация таргетов (локальные словари + API-фолбэк);
  - PubChem — обогащение молекул.

- **Фиксация релизов и параметры**:
  - ChEMBL Release: использовать значение из статуса API; в примерах — Release 36 (актуально на октябрь 2025).
  - Для вторичных источников фиксируются дата выгрузки/снимка, лимиты и ретраи (см. `configs/config_*.yaml`, `docs/configuration/sources.md`).

- **Ограничения API**:
  - Лимиты скорости и ретраи конфигурируются глобально и для источников: `http.global.*`, `sources.*.http.*`, `sources.*.rate_limit`;
  - Ключи API (опционально) — через env-переменные и плейсхолдеры в заголовках (например, `Authorization: "Bearer {CHEMBL_API_TOKEN}"`, `x-api-key: {SEMANTIC_SCHOLAR_API_KEY}`).

См. подробности в `docs/configuration/sources.md` и `configs/config_*.yaml`.

---

### 03. Основная модель данных

Модель — звёздная схема вокруг таблицы фактов активности с измерениями: документы, мишени, ассейи, молекулы.

- **Сущности**:
  - Документы: `document_chembl_id`, `doi`, `title`, `journal`, `year`, `document_pubmed_id`;
  - Мишени: `target_chembl_id`, `pref_name`, `organism`, UniProt-мэппинги и классификации IUPHAR;
  - Ассейи: `assay_chembl_id`, `target_chembl_id`, `assay_type`, BAO-поля, описание;
  - Тестовые объекты (молекулы): `molecule_chembl_id`, `canonical_smiles`, `molecule_type`, `full_mwt`, `parent_molecule_chembl_id`;
  - Активности: `activity_id`, `assay_chembl_id`, `molecule_chembl_id`, `target_chembl_id`, `document_chembl_id`, `standard_value`, `standard_units`, `pchembl_value`, `standard_relation` и др.

- **Ключи и зависимости**:
  - PK на естественных идентификаторах из ChEMBL; FK обеспечивают ссылочную целостность между фактами и измерениями;
  - для устойчивой дедупликации используются `hash_row` и `hash_business_key` (SHA256) в нормализованных наборах (см. `src/library/schemas/activity_schema.py`, `testitem_schema.py`).

- **Схемы и валидация**:
  - Pandera-схемы и проверки в `src/library/schemas/` (activity, assay, testitem, document input);
  - дополнительные бизнес-правила — в пайплайнах и постобработке (`postprocessing`, `data_validator`).

См. также `docs/star-schema.md`.

---

### 04. Процесс извлечения данных (Workflow)

Последовательность: Документы → Мишени → Ассейи → Тестовые объекты → Активности. Каждый шаг автономен, но данные согласованы через идентификаторы.

- **Общие положения**:
  - Конфигурации — YAML (`configs/config_*.yaml`), с переопределениями через CLI и env (`BIOACTIVITY__...`).
  - Детерминированная запись артефактов и авто-генерация QC/мета.
  - Логи — структурированные, поэтапно.

#### 4.1 Документы

- Вход: `data/input/documents.csv` с полями `document_chembl_id`, `doi`, `title`.
- Источники: ChEMBL, Crossref, OpenAlex, PubMed, Semantic Scholar (включаются конфигом).
- Выход: `data/output/documents/documents_{YYYYMMDD}.csv` + `documents_{YYYYMMDD}_qc.csv` + `documents_{YYYYMMDD}_meta.yaml`.
- Команда:

```bash
python src/scripts/get_document_data.py \
  --input data/input/documents.csv \
  --config configs/config_documents_full.yaml \
  --output-dir data/output/documents/
```

#### 4.2 Мишени

- Вход: `data/input/target.csv` (колонка ID задаётся через `--id-column`, по умолчанию из профиля режима).
- Источники: ChEMBL (core), UniProt (обогащение), IUPHAR (классификация; локальные CSV + API fallback).
- Выход: `data/output/target/target_{YYYYMMDD}.csv` + QC + meta.
- Команда:

```bash
python src/scripts/get_target_data.py \
  --input data/input/target.csv \
  --config configs/config_target_full.yaml \
  --output data/output/target/target_{YYYYMMDD}.csv
```

#### 4.3 Ассейи

- Вход: `data/input/assay.csv` (колонка `assay_chembl_id`) или параметр `--target CHEMBL...`.
- Фильтры: профили в конфиге (например, `human_single_protein`, `binding_assays`, `functional_assays`, `high_quality`).
- Источники: ChEMBL.
- Выход: `data/output/assay/assays_{YYYYMMDD}.csv` + QC + meta.
- Команды:

```bash
python src/scripts/get_assay_data.py \
  --input data/input/assay.csv \
  --config configs/config_assay_full.yaml

python src/scripts/get_assay_data.py \
  --target CHEMBL231 \
  --filters human_single_protein \
  --config configs/config_assay_full.yaml
```

#### 4.4 Тестовые объекты (Молекулы)

- Вход: `data/input/testitem.csv` (хотя бы одна колонка из: `molecule_chembl_id`, `molregno`).
- Источники: ChEMBL; PubChem — опционально (включается через `--disable-pubchem`/конфиг).
- Выход: `data/output/testitem/testitems_{YYYYMMDD}.csv` + QC + meta.
- Команда:

```bash
python src/scripts/get_testitem_data.py \
  --input data/input/testitem.csv \
  --config configs/config_testitem_full.yaml \
  --output-dir data/output/testitem/
```

#### 4.5 Активности

- Вход: `data/input/activity.csv` (как минимум идентификатор активности/связанные ключи, см. конфиг).
- Источники: ChEMBL.
- Выход: `data/output/activity/activities_{YYYYMMDD}.csv` + QC + meta.
- Команда:

```bash
python src/scripts/get_activity_data.py \
  --input data/input/activity.csv \
  --config configs/config_activity_full.yaml \
  --output-dir data/output/activity/
```

---

### 05. Контроль качества (QC)

- **Валидация схемы**: Pandera-схемы для `activity`, `assay`, `testitem`, входных `document`-данных; дополнительные проверки через `library.tools.data_validator`.
- **Базовые метрики**: `build_qc_report()` — число строк, пропуски ключей, дубликаты (для документов — по `document_chembl_id`, `doi`, `title`).
- **Расширенный QC**: `enhanced_qc` — паттерны (DOI/ISSN/ISBN/URL), роли данных, статистики, распределения.
- **Цензурированные и нормализованные поля**: ведутся при нормализации активностей (отношения, пределы); покрытие нормализованных единиц и рассчитанных `pchembl_value` отражается в отчётах.
- **Пороговые значения**: применяются при генерации QC (см. `QCValidationSettings`), статусы pass/fail включаются в итоговый отчёт.
- **Отчётность**: авто-генерация `*_qc.csv`, `*_meta.yaml`, а также директорий корреляционных отчётов при включённой опции.

---

### 06. Процедуры нормализации

- **Единицы измерения**: приведение `standard_value/units` к унифицированным единицам (например, нМ для активностей) с сохранением исходных значений и отношения (`standard_relation`).
- **Стандартизация полей**: приведение типов, нормализация строк/списков, выравнивание колоночного набора между источниками.
- **Хэши для дедупликации**: вычисляются `hash_row`, `hash_business_key` (SHA256) на нормализованных наборах; дубликаты удаляются детерминированно.
- **Обработка пропусков и цензуры**: заполнение дефолтами, явные флаги и расчёт верхних/нижних границ (если применимо).
- **Постобработка таргетов**: унификация и классификация (UniProt/IUPHAR), вычисление предикатов `protein_class_pred`.

См. реализацию правил в пайплайнах (`src/library/*/pipeline.py`) и утилитах `etl/transform.py`, `pipelines/target/postprocessing.py`.

---

### 07. Представление и отчётность данных

- **Форматы**: CSV — основной; Parquet — опционально; YAML — метаданные.
- **Детерминизм**: `write_deterministic_csv` обеспечивает фиксированный порядок колонок и стабильную сортировку (набор и порядок задаются конфигом/кодом).
- **Структура директорий**:
  - `data/input/` — входные CSV;
  - `data/output/{documents|target|assay|testitem|activity}/` — артефакты пайплайнов;
  - `logs/` — журналы выполнения;
  - `configs/` — конфигурации YAML.
- **Сводные метрики** (в meta): `row_count`, `chembl_release`, `enabled_sources`, `extraction_parameters`, `file_checksums`.
- **Визуализация**: распределения (например, `pchembl_value`), тренды публикаций по годам, таблицы QC, диаграммы workflow.

---

### 08. Приложения

#### Приложение A — Схемы выходных данных (Pandera)

- Документы (вход): `DocumentInputSchema` — обязательные `document_chembl_id`, `title`; опциональные `doi`, `document_pubmed_id`, `journal`, `year` и др.
- Мишени: итоговые колонки выравниваются постпроцессингом; порядок — по `library.schemas.targets`.
- Ассейи: `AssayInputSchema` (вход), `AssayNormalizedSchema` (выход) — нормализация типов и диапазонов, BAO-поля.
- Тестовые объекты: `TestitemInputSchema`, `TestitemRawSchema` — входные идентификаторы и нормализованные поля, расчёт хэшей.
- Активности: `RawActivitySchema`, `NormalizedActivitySchema` — обязательные служебные поля (`source_system`, `chembl_release`, `source`, `retrieved_at`), нормализованные значения, `pchembl_value`, хэши.

См. `src/library/schemas/*.py`.

#### Приложение B — Диаграммы пайплайна

- Workflow: Документы → Мишени → Ассейи → Тестовые объекты → Активности → QC/Отчёты.
- Граф зависимостей: Активности ссылаются на все измерения; Ассейи — на Мишени.
- Звёздная схема: центральная таблица фактов активности, радиальные измерения документов, мишеней, ассейев, молекул.

#### Приложение C — Примеры YAML-конфигураций

```yaml
http:
  global:
    timeout_sec: 60.0
    retries:
      total: 5
      backoff_multiplier: 2.0

sources:
  chembl:
    name: chembl
    endpoint: document
    http:
      base_url: https://www.ebi.ac.uk/chembl/api/data
      headers:
        Authorization: "Bearer {CHEMBL_API_TOKEN}"

io:
  output:
    dir: data/output/documents
    format: csv

runtime:
  limit: 1000
  dry_run: false
```

#### Приложение D — Примеры команд CLI

```bash
# Документы
python src/scripts/get_document_data.py --input data/input/documents.csv --config configs/config_documents_full.yaml --output-dir data/output/documents/

# Мишени
python src/scripts/get_target_data.py --input data/input/target.csv --config configs/config_target_full.yaml --output data/output/target/target_{YYYYMMDD}.csv

# Ассейи
python src/scripts/get_assay_data.py --input data/input/assay.csv --config configs/config_assay_full.yaml
python src/scripts/get_assay_data.py --target CHEMBL231 --filters human_single_protein --config configs/config_assay_full.yaml

# Молекулы
python src/scripts/get_testitem_data.py --input data/input/testitem.csv --config configs/config_testitem_full.yaml --output-dir data/output/testitem/

# Активности
python src/scripts/get_activity_data.py --input data/input/activity.csv --config configs/config_activity_full.yaml --output-dir data/output/activity/
```

#### Приложение E — Примеры отчетов QC

- Сводная статистика: число строк, пропуски ключей, дубликаты, покрытие нормализации, статус `qc_passed`.
- Корреляционные отчёты: директории `*_correlation_report_*` при включенном `postprocess.correlation.enabled`.

#### Приложение F — Примеры ассейев киназ

- Примеры из набора киназ (ABL1, AKT1, ALK и т.д.) для иллюстрации фильтрации по виду/типу ассейя (используйте `organism="Homo sapiens"`, профиль `human_single_protein`).

---

### Примечания по уточнению (v0.5)

- Обновлены примеры до ChEMBL Release 36 (октябрь 2025).
- Перенастроены зависимые разделы под фактический код: собственные HTTP-клиенты вместо `chembl_webresource_client`, ретраи через `backoff`.
- Нормализация и QC приведены к единому стандарту для всех 5 пайплайнов.
-- Документ полностью на русском языке.
