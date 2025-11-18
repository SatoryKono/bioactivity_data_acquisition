## Сводка
1. Репозиторий уже использует layout `src/` + `tests/`, но внутри пакета `bioetl` смешаны базовые слои (core, config, schemas), инфраструктурные клиенты и конкретные пайплайны, что затрудняет навигацию и переиспользование.
2. Пакет `pipelines` объединяет абстрактные классы (`base.py`, `mixins.py`, `unified_base.py`) и конкретные Chembl-пайплайны, поэтому границы ответственности размыты.
3. Общая ChEMBL-инфраструктура (дескрипторы, клиенты) распылена между `chembl/`, `pipelines/chembl/` и общими `clients/`/`sources/`, что мешает единообразию конфигурации.
4. Каталоги `clients`, `sources`, `tools` лежат на одном уровне с доменной логикой, хотя относятся к инфраструктурному слою и могут переиспользоваться вне ChEMBL.
5. Текущие тесты отражают прежнюю структуру (`tests/bioetl`, `tests/pipelines`), поэтому после миграции потребуется перенастройка импортов и фикстур.
6. Целевая структура выделяет явные уровни: `core` (базовые классы, конфигурация, схемы), `infrastructure` (клиенты, источники, инструменты), `chembl` (общие ChEMBL-артефакты), `pipelines` (база + конкретные реализации) и согласованный набор тестов.
7. План миграции предполагает пошаговое перемещение: сначала ядро/инфраструктура, затем пайплайны и тесты, с обязательным обновлением импортов и проверкой CLI.

## Текущая структура (укрупнённо)
```
bioactivity_data_acquisition/
├── configs/
├── data/
├── docs/
├── scripts/
├── src/
│   ├── bioetl/
│   │   ├── base_classes.py, cli/, config/, schemas
│   │   ├── chembl/common/, clients/, sources/, tools/, utils/
│   │   ├── pipelines/{base.py, mixins.py, unified_base.py, chembl/, common/, qc/}
│   │   └── vocab/, core/, devtools/, qc/
│   └── typecheck/
├── tests/
│   ├── bioetl/
│   ├── pipelines/
│   ├── support/, golden/
│   └── conftest.py
├── typings/
└── pyproject.toml, Makefile, README.md, ...
```

## Целевая структура
```
bioactivity_data_acquisition/
├── configs/
├── src/
│   └── bioetl/
│       ├── core/
│       │   ├── base_classes.py
│       │   ├── config/
│       │   └── schemas.py
│       ├── infrastructure/
│       │   ├── clients/
│       │   ├── sources/
│       │   └── tools/
│       ├── chembl/
│       │   ├── common/
│       │   └── clients/
│       ├── pipelines/
│       │   ├── base/
│       │   │   ├── __init__.py (PipelineBase, UnifiedPipelineBase, mixins)
│       │   ├── chembl/
│       │   ├── qc/
│       │   └── common/
│       ├── utils/
│       └── cli/
├── tests/
│   ├── core/
│   ├── infrastructure/
│   ├── chembl/
│   └── pipelines/
└── scripts/, docs/, data/, typings/
```

## План изменений по файлам
| Текущий путь | Новый путь | Причина | Комментарий |
| --- | --- | --- | --- |
| `src/bioetl/base_classes.py` | `src/bioetl/core/base_classes.py` | Базовые сущности ядра нужно собрать в отдельном пакете | Обновить все импорты пайплайнов, CLI и тестов |
| `src/bioetl/config/**` | `src/bioetl/core/config/**` | Конфигурация относится к ядру | Провести `rg` по `bioetl.config` и заменить |
| `src/bioetl/schemas.py` | `src/bioetl/core/schemas.py` | Схемы — часть core | Перенастроить проверки типов и тесты |
| `src/bioetl/clients/**` | `src/bioetl/infrastructure/clients/**` | Выделение инфраструктурного слоя | Убедиться, что нет относительных импортов из `bioetl.clients` |
| `src/bioetl/sources/**` | `src/bioetl/infrastructure/sources/**` | Источники данных — инфраструктура | Обновить пайплайны и utils |
| `src/bioetl/tools/**` | `src/bioetl/infrastructure/tools/**` | Служебные инструменты ближе к интеграциям | Проверить devtools и CLI |
| `src/bioetl/pipelines/base.py` | `src/bioetl/pipelines/base/__init__.py` | Сгруппировать PipelineBase, BatchRunner и mixins | Вынести `mixins.py` и `unified_base.py` в этот пакет |
| `src/bioetl/pipelines/mixins.py` | `src/bioetl/pipelines/base/mixins.py` | Хранить mixins рядом с базовым классом | Импорты из конкретных пайплайнов |
| `src/bioetl/pipelines/unified_base.py` | `src/bioetl/pipelines/base/unified.py` | Держать UnifiedPipelineBase в пакете base | Обновить экспорт `__all__` |
| `src/bioetl/chembl/common/**` + ChEMBL-клиенты из `clients/` | `src/bioetl/chembl/{common,clients}/**` | Сфокусировать ChEMBL-инфраструктуру | Оставить единые точки импорта `bioetl.chembl.*` |
| `tests/bioetl/**` | `tests/core/**` | Тесты должны следовать структуре `src` | Переместить фикстуры и обновить пути |
| `tests/pipelines/**` | `tests/pipelines/**` (обновить импорты) | Отразить новое расположение модулей | Проверить `pytest.ini` |

## Замечания по миграции
1. Переезд выполнять батчами: (а) ядро и конфигурация; (б) инфраструктурные клиенты/источники/инструменты; (в) ChEMBL-клиенты; (г) пакет `pipelines`; (д) тесты. Это уменьшит объем правок в каждом PR.
2. После каждого шага запускать `pytest`, `mypy`, `ruff` и smoke-тест CLI, чтобы зафиксировать корректность импортов и публичных контрактов.
3. Для перемещения модулей использовать `git mv`, чтобы сохранить историю и упростить ревью.
4. Внимательно проверить точки входа CLI и автоматизации (`scripts/`, `Makefile`), которые могут ссылаться на старые пути.
5. После обновления тестовой структуры убедиться, что фикстуры из `tests/support` по-прежнему доступны, и при необходимости обновить `pytest.ini` или `conftest.py`.
