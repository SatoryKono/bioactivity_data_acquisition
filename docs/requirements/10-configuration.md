# 10. Единый стандарт конфигурации ETL-пайплайна

## 1. Общее описание

Все пайплайны используют единую систему конфигурации на базе YAML и Pydantic. Базовый файл `configs/base.yaml` описывает обязательные секции, а профильные конфигурации (например, `configs/pipelines/assay.yaml`) расширяют его через механизм наследования. Переопределения допускаются через CLI и переменные окружения с приоритетом `base.yaml < profile.yaml < CLI < env`.

**Цели стандарта:**

- типобезопасность и единая точка валидации;

- детерминированная сериализация конфигураций (hash → `config_hash`);

- предсказуемые правила расширения и приоритетов;

- единая схема логических секций (`http`, `cache`, `determinism`, `postprocess`, `paths`, `qc`, `cli`).

### 1.1 Инвентаризация пайплайнов

Для контроля актуальности конфигураций и связанных с ними модулей введён детерминированный отчёт `docs/requirements/PIPELINES.inventory.csv`. Артефакт фиксирует текущее состояние модулей и конфигураций и генерируется командой `python src/scripts/run_inventory.py --config configs/inventory.yaml`, которая анализирует дерево `src/`, `tests/`, `docs/requirements/` и каталог конфигураций `src/bioetl/configs/pipelines`. Скрипт вычисляет LOC, публичные символы, импорты и ключи YAML-файлов, а также строит кластеризацию по n-граммам и импортам. Результат кластеризации сохраняется в `docs/requirements/PIPELINES.inventory.clusters.md` и обновляется вместе с CSV.

Хук pre-commit и пайплайн CI (`python src/scripts/run_inventory.py --check --config configs/inventory.yaml`) гарантируют, что коммиты не нарушают слепок инвентаризации. Таким образом документация и конфигурации всегда согласованы с текущим состоянием кода.

## 2. Базовая схема YAML

Базовый файл `configs/base.yaml` содержит каркас секций и обязательные поля.

```yaml

version: 1
pipeline:
  name: "base"
  entity: "abstract"
  release_scope: true  # связывать конфиг с версией источника

http:
  global:
    timeout_sec: 60.0
    retries:
      total: 5
      backoff_multiplier: 2.0
      backoff_max: 120.0
    rate_limit:
      max_calls: 5
      period: 15.0
cache:
  enabled: true
  directory: "data/cache"
  ttl: 86400
  release_scoped: true
paths:
  input_root: "data/input"
  output_root: "data/output"
determinism:
  sort:
    by: []
    ascending: []
  column_order: []
postprocess:
  qc:
    enabled: true
qc:
  severity_threshold: "warning"
cli:
  default_config: "configs/base.yaml"

```

### 2.1 Обязательные поля и их назначение

| Секция        | Поле                                   | Назначение                                                       |
|---------------|----------------------------------------|------------------------------------------------------------------|
| `version`     | целое                                  | Версионирование схемы конфигурации.                              |
| `pipeline`    | `name`, `entity`                       | Идентификация пайплайна (используется в логах, метаданных).      |
| `http.global` | `timeout_sec`, `retries.total`         | Гарантированные лимиты для клиентов без локальных переопределений. |
| `cache`       | `enabled`, `directory`, `release_scoped` | Политика кэширования и инвалидации.                              |
| `paths`       | `output_root`                          | Каталог для детерминированных артефактов.                        |
| `determinism` | `sort.by`, `column_order`              | Формирование стабильного порядка строк/столбцов.                 |
| `postprocess` | `qc.enabled`                           | Включение QC-этапов.                                             |
| `qc`          | `severity_threshold`                   | Глобальный уровень, при превышении которого пайплайн падает.     |
| `cli`         | `default_config`                       | Значение по умолчанию для `--config`.                            |

Дополнительные секции (`sources`, `enrichment`, `integrations`) добавляются профильными конфигурациями, но должны быть описаны в разделе 5.

## 3. Правила наследования и расширений

Каждый профильный файл должен объявлять, от какого шаблона он расширяется, с помощью ключа `extends`:

```yaml

# configs/pipelines/assay.yaml

extends:

  - "../base.yaml"
  - "../includes/determinism.yaml"

pipeline:
  name: "assay"
  entity: "assay"

sources:
  chembl:
    enabled: true
    base_url: "https://www.ebi.ac.uk/chembl/api/data"
    batch_size: 25
    max_url_length: 2000

determinism:
  sort:
    by: ["assay_chembl_id"]
    ascending: [true]
  column_order: ["assay_chembl_id", "pipeline_version", "hash_row", "hash_business_key"]

```

Вынесенный include `configs/includes/determinism.yaml` задаёт единые значения `hash_algorithm`, `float_precision` и `datetime_format`,
а конкретный пайплайн отвечает только за собственные ключи сортировки и порядок столбцов.

Мерж выполняется по правилам «глубокого» обновления словарей:

- словари объединяются рекурсивно;

- списки считаются атомарными и полностью заменяются дочерними значениями (например, `determinism.sort.by` в профиле полностью заменяет базовый список);

- скалярные значения заменяются последним источником.

Профильные файлы могут ссылаться на общие шаблоны через `anchors` и `aliases`, однако итоговая конфигурация после развёртывания должна соответствовать модели из раздела 4.

## 4. Pydantic-модели

Конфигурация загружается через корневую модель `PipelineConfig`, которая строится из вложенных Pydantic-моделей.

```python

from __future__ import annotations
from pathlib import Path
from typing import Dict, List, Optional
from pydantic import BaseModel, Field, validator

class RetryConfig(BaseModel):
    total: int = Field(..., ge=0)
    backoff_multiplier: float = Field(..., gt=0)
    backoff_max: float = Field(..., gt=0)

class RateLimitConfig(BaseModel):
    max_calls: int = Field(..., ge=1)
    period: float = Field(..., gt=0)

class HTTPSection(BaseModel):
    timeout_sec: float = Field(..., gt=0, env="HTTP__GLOBAL__TIMEOUT_SEC")
    retries: RetryConfig
    rate_limit: RateLimitConfig
    headers: Dict[str, str] = Field(default_factory=dict)

class SourceConfig(BaseModel):
    enabled: bool = True
    base_url: str
    batch_size: Optional[int] = Field(default=None, ge=1)
    extra: Dict[str, object] = Field(default_factory=dict, alias="*")

class CacheConfig(BaseModel):
    enabled: bool
    directory: Path
    ttl: int = Field(..., ge=0)
    release_scoped: bool

class DeterminismConfig(BaseModel):
    sort_by: List[str] = Field(..., alias="sort.by")
    column_order: List[str]

class PipelineMetadata(BaseModel):
    name: str
    entity: str
    release_scope: bool = True

class PipelineConfig(BaseModel):
    version: int
    pipeline: PipelineMetadata
    http: Dict[str, HTTPSection]
    sources: Dict[str, SourceConfig] = Field(default_factory=dict)
    cache: CacheConfig
    paths: Dict[str, Path]
    determinism: DeterminismConfig
    postprocess: Dict[str, object] = Field(default_factory=dict)
    qc: Dict[str, object] = Field(default_factory=dict)
    cli: Dict[str, object] = Field(default_factory=dict)

    @validator("version")
    def check_version(cls, value: int) -> int:
        if value != 1:
            raise ValueError("Unsupported config version")
        return value

```

Модели используют `env` и `alias` для поддержки плоских переопределений. При сериализации в YAML необходимо применять `model_dump()` и `yaml.safe_dump` с `sort_keys=True`.

## 5. Переопределения через CLI и окружение

### 5.1 CLI

CLI (`bioetl pipeline run`) поддерживает опцию `--set <path>=<value>`:

```bash

bioetl pipeline run \
  --config configs/pipelines/assay.yaml \
  --set sources.chembl.batch_size=20 \
  --set http.global.timeout_sec=45

```

Путь интерпретируется точечной нотацией и применяется после загрузки профильного файла. Для сложных структур допускается передача JSON-строки: `--set determinism.sort.by='["assay_chembl_id"]'`.

### 5.2 Переменные окружения

Каждое поле может быть переопределено через переменные окружения с двойным подчёркиванием в качестве разделителя:

| Переменная                   | Эквивалентный путь                |
|------------------------------|-----------------------------------|
| `BIOETL_HTTP__GLOBAL__TIMEOUT_SEC` | `http.global.timeout_sec`          |
| `BIOETL_SOURCES__CHEMBL__API_KEY`  | `sources.chembl.api_key`           |
| `BIOETL_CACHE__DIRECTORY`          | `cache.directory`                  |
| `BIOETL_QC__MAX_TITLE_FALLBACK`    | `qc.max_title_fallback`            |

Переменные окружения применяются **после** CLI-переопределений, что позволяет секьюрно прокидывать секреты на уровне CI/CD.

### 5.3 CLI Interface Specification (AUD-4)

**Унифицированные CLI флаги для всех пайплайнов:**

Все пайплайны (assay, activity, testitem, target, document) поддерживают следующий набор стандартных флагов:

| Флаг | Тип | Обязательность | Описание |
|---|---|---|---|
| `--config` | path | Опционально | Путь к YAML конфигурации (default: `configs/base.yaml`) |
| `--golden` | path | Опционально | Путь к golden-файлу для детерминированного сравнения |
| `--sample` | int | Опционально | Ограничить входные данные до N записей (для тестирования) |
| `--fail-on-schema-drift` | flag | Опционально | Fail-fast при major-версии схемы (default: `True` в production) |
| `--extended` | flag | Опционально | Включить расширенные артефакты (correlation_report, meta.yaml, manifest) |
| `--mode` | str | Опционально | Режим работы (для Document: `chembl` | `all`) |
| `--dry-run` | flag | Опционально | Проверка конфигурации без выполнения |
| `--verbose` / `-v` | flag | Опционально | Детальное логирование |

**Приоритет переопределений:**

1. Базовый конфиг (`base.yaml`)
2. Профильный конфиг (`assay.yaml`, `activity.yaml`, и т.д.)
3. CLI флаги (`--config`, `--set`)
4. Переменные окружения (`BIOETL_*`)

**Инварианты CLI:**

```python

@dataclass
class CLIArguments:
    """Модель CLI аргументов для всех пайплайнов."""
    config: Path
    golden: Path | None = None
    sample: int | None = None
    fail_on_schema_drift: bool = True  # Production default
    extended: bool = False
    mode: str = "default"
    dry_run: bool = False
    verbose: bool = False

    # Дополнительные аргументы через --set

    overrides: dict[str, Any] = field(default_factory=dict)

    def validate(self) -> None:
        """Валидация конфликтующих опций."""
        if self.sample and self.sample < 1:
            raise ValueError("--sample must be >= 1")
        if self.config and not self.config.exists():
            raise FileNotFoundError(f"Config not found: {self.config}")

```

**Таблица поддержки флагов по пайплайнам:**

| Флаг | Assay | Activity | Testitem | Target | Document |
|---|---|---|---|---|---|
| `--config` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `--golden` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `--sample` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `--fail-on-schema-drift` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `--extended` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `--mode` | ❌ | ❌ | ❌ | ❌ | ✅ (`chembl` \| `all`) |

**Примеры использования:**

```bash

# Базовый запуск

bioetl pipeline run --config configs/pipelines/assay.yaml

# С golden-сравнением

bioetl pipeline run --config configs/pipelines/activity.yaml \
  --golden data/golden/activity_20241021.csv

# Расширенный режим с переопределением

bioetl pipeline run --config configs/pipelines/document.yaml \
  --mode all \
  --extended \
  --set sources.crossref.batching.dois_per_request=50

# Тестовый запуск на ограниченной выборке

bioetl pipeline run --config configs/pipelines/testitem.yaml \
  --sample 100 \
  --verbose

```

**Ссылка:** См. секции CLI в [05-assay-extraction.md](05-assay-extraction.md), [06-activity-data-extraction.md](06-activity-data-extraction.md), [07a-testitem-extraction.md](07a-testitem-extraction.md), [08-target-data-extraction.md](08-target-data-extraction.md), [09-document-chembl-extraction.md](09-document-chembl-extraction.md).

## 6. Профильные расширения

| Профиль | Файл | Ключевые расширения | Ограничения |
|---------|------|---------------------|-------------|
| Assay | `configs/pipelines/assay.yaml` | `sources.chembl.batch_size ≤ 25`, `determinism.sort.by` фиксирует `assay_chembl_id` как первый ключ | Требует `sources.chembl.max_url_length` и `cache.namespace="chembl"`. |
| Activity | `configs/pipelines/activity.yaml` | `postprocess.normalization.activity_units`, `qc.max_ic50_missing` | Должен ссылаться на `determinism.column_order` из справочника. |
| Test Item | `configs/pipelines/testitem.yaml` | `sources.pubchem.enabled`, `enrichment.pubchem.ttl_hours` | Обязателен `sources.chembl.batch_size=25`. |
| Target | `configs/pipelines/target.yaml` | `sources.uniprot.id_mapping_max_ids`, `sources.iuphar.retries` | TTL для каждого источника объявляется в `cache.ttl.<source>`. |
| Document (ChEMBL + PubMed) | `configs/pipelines/document.yaml` | `sources.pubmed.history.use_history`, `sources.crossref.identify.mailto_env`, `postprocess.priority_matrix` | Требуются `qc.max_title_fallback`, `qc.max_s2_access_denied`. |

Каждый профиль обязан документировать собственные расширения в соответствующем разделе требований и ссылаться на настоящий стандарт вместо дублирования YAML.

## 7. Контроль соответствия

- Любой новый профиль обязан:

  1. объявить `extends` на `../base.yaml` или иной общий шаблон;
  2. пройти валидацию `PipelineConfig.validate_yaml(path)` (реализация в `src/config/loader.py`);
  3. задокументировать специфичные ограничения (batch size, TTL, сортировка).

- Набор линтеров (`ruff`, `mypy`) должен проверять, что `PipelineConfig` не допускает неизвестных полей (`model_config = ConfigDict(extra="forbid")`).

Применение этого стандарта обеспечивает единообразие конфигураций, избавляет от копипасты YAML и упрощает сопровождение CLI/CI-переопределений.

